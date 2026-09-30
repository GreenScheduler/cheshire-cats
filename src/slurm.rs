//! Safe wrapper over the libslurm calls for our daemon.
//!
//! Everything here goes through the public, versioned libslurm API
//! (`slurm_load_node`, `slurm_update_node`), which sends RPCs to slurmctld.
//! Updates are refused unless the process runs as root or SlurmUser.

use std::collections::HashSet;
use std::ffi::{CStr, CString, c_char};
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr;

use crate::gate::{Controller, Lease};
use crate::node::{NodeState, NodeView};

#[derive(Debug)]
pub struct SlurmError {
    op:  &'static str,
    code: i32,
    msg:  String,
}

impl SlurmError {
    /// Builds the error from the errno libslurm left behind. Call this
    /// immediately after the failing libslurm call.
    fn last(op: &'static str) -> Self {
        let code = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        let msg  = unsafe { opt_string( slurm_sys::slurm_strerror(code) ) }
            .unwrap_or_else( || "unknown error".into() );
        Self { op, code, msg }
    }

    fn nul_byte(op: &'static str) -> Self {
        Self {
            op,
            code: libc::EINVAL,
            // NB: C string terminator (\0) is NUL not NULL
            msg:  "argument contains a NUL byte".into(),
        }
    }

    #[cfg(test)]
    pub fn for_test(op: &'static str) -> Self {
        Self {
            op,
            code: 0,
            msg:  "simulated failure".into(),
        }
    }
}

impl fmt::Display for SlurmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} (error {})", self.op, self.msg, self.code)
    }
}

impl std::error::Error for SlurmError {}

/// Copies a C string owned by libslurm; NULL becomes `None`.
unsafe fn opt_string(p: *const c_char) -> Option<String> {
    if p.is_null() {
        None
    } else {
        Some( unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned() )
    }
}

/// Expands a hostlist expression such as `node[01-04],gpu7` into names.
/// Purely local; no RPC.
pub fn expand_hostlist(expr: &str) -> Result<Vec<String>, SlurmError> {
    const OP: &str = "parse node list";
    let c_expr     = CString::new(expr).map_err( |_| SlurmError::nul_byte(OP) )?;
    let hl         = unsafe { slurm_sys::slurm_hostlist_create( c_expr.as_ptr() ) };
    if hl.is_null() {
        return Err( SlurmError::last(OP) );
    }
    let mut names = Vec::new();
    loop {
        let host = unsafe { slurm_sys::slurm_hostlist_shift(hl) };
        if host.is_null() {
            break;
        }
        // Allocated with plain malloc(), so released with free().
        names.extend(unsafe { opt_string(host) });
        unsafe { libc::free( host.cast() ) };
    }
    unsafe { slurm_sys::slurm_hostlist_destroy(hl) };
    Ok(names)
}

/// An initialized libslurm. Dropping it calls `slurm_fini()`.
///
/// Not `Send`: libslurm keeps process-global state, and the daemon makes all
/// of its calls from the main thread.
pub struct Slurm {
    _not_send: PhantomData<*const ()>,
}

impl Slurm {
    /// Loads slurm.conf (from `conf`, else `SLURM_CONF`, else the built-in
    /// default path). libslurm exits the process if the configuration cannot
    /// be read.
    pub fn init(conf: Option<&Path>) -> Result<Self, SlurmError> {
        let conf = conf
            .map( |p| CString::new( p.as_os_str().as_bytes() ) )
            .transpose()
            .map_err( |_| SlurmError::nul_byte("read slurm.conf path") )?;
        unsafe { slurm_sys::slurm_init( conf.as_ref().map_or( ptr::null(), |c| c.as_ptr() ) ) };
        Ok(Self {
            _not_send: PhantomData,
        })
    }

    fn update(
        &mut self,
        names:        &[&str],
        state:        u32,
        reason:       Option<&str>,
        resume_after: Option<u32>,
    ) -> Result<(), SlurmError> {
        const OP: &str = "update nodes";
        let node_names = CString::new( names.join(",") ).map_err( |_| SlurmError::nul_byte(OP) )?;
        let reason     = reason
            .map(CString::new)
            .transpose()
            .map_err( |_| SlurmError::nul_byte(OP) )?;

        let mut msg = MaybeUninit::<slurm_sys::update_node_msg_t>::zeroed();
        // Sets every field to its "not being changed" sentinel.
        unsafe { slurm_sys::slurm_init_update_node_msg( msg.as_mut_ptr() ) };
        let mut msg = unsafe { msg.assume_init() };
        // libslurm only reads these strings; the casts drop a const it lacks.
        msg.node_names = node_names.as_ptr().cast_mut();
        msg.node_state = state;
        if let Some(r) = &reason {
            msg.reason = r.as_ptr().cast_mut();
        }
        if let Some(secs) = resume_after {
            msg.resume_after = secs;
        }

        if unsafe { slurm_sys::slurm_update_node(&mut msg) } == slurm_sys::SLURM_SUCCESS as i32 {
            Ok( () )
        } else {
            Err( SlurmError::last(OP) )
        }
    }
}

impl Drop for Slurm {
    fn drop(&mut self) {
        unsafe { slurm_sys::slurm_fini() };
    }
}

/// Frees a `slurm_load_node` response when it goes out of scope.
struct NodeInfoMsg(*mut slurm_sys::node_info_msg_t);

impl Drop for NodeInfoMsg {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { slurm_sys::slurm_free_node_info_msg(self.0) };
        }
    }
}

impl Controller for Slurm {
    fn load(&mut self, names: &[&str]) -> Result<Vec<NodeView>, SlurmError> {
        // The reply lists every node in the cluster; look ours up in O(1).
        let wanted: HashSet<&str> = names.iter().copied().collect();
        let mut raw = ptr::null_mut();
        // update_time 0: always return the full table. SHOW_ALL includes
        // nodes in hidden partitions.
        if unsafe { slurm_sys::slurm_load_node(0, &mut raw, slurm_sys::SHOW_ALL as u16) }
            != slurm_sys::SLURM_SUCCESS as i32
        {
            return Err(SlurmError::last("load nodes"));
        }
        let msg = NodeInfoMsg(raw);
        if msg.0.is_null() {
            return Ok( Vec::new() );
        }
        let (node_array, count) = unsafe { ( (*msg.0).node_array, (*msg.0).record_count ) };
        if node_array.is_null() {
            return Ok( Vec::new() );
        }

        let mut node_views = Vec::with_capacity( names.len() );
        for each_node in unsafe { std::slice::from_raw_parts(node_array, count as usize) } {
            let Some(name) = (unsafe { opt_string(each_node.name) }) else {
                continue;
            };
            if !wanted.contains(name.as_str()) {
                continue;
            }
            node_views.push(NodeView {
                name,
                state:        NodeState(each_node.node_state),
                reason:       unsafe { opt_string(each_node.reason) },
                resume_after: each_node.resume_after,
            });
        }
        Ok(node_views)
    }

    /// `NODE_STATE_DRAIN`: no new jobs are scheduled, running jobs finish.
    ///
    /// On an already-drained node slurmctld treats this as an equivalent
    /// state change: only the timer (and the reason, if given) is updated,
    /// making lease renewal cheap. `resume_after` is relative
    /// seconds, and is honored only if DRAIN or DOWN are also set.
    fn drain(
        &mut self,
        names:  &[&str],
        reason: Option<&str>,
        lease:  Lease,
    ) -> Result<(), SlurmError> {
        let resume_after = match lease {
            Lease::Seconds(s) => s,
            Lease::Cancel     => slurm_sys::INFINITE,
        };
        self.update( names, slurm_sys::NODE_STATE_DRAIN, reason, Some(resume_after) )
    }

    /// `NODE_STATE_UNDRAIN` clears only the DRAIN flag, leaving
    /// a node that went DOWN in the meantime DOWN. It also clears a
    /// pending `resume_after`, canceling our lease.
    /// NOTE: undraining does not power a node up; that needs `NODE_STATE_POWER_UP`.
    fn undrain(&mut self, names: &[&str]) -> Result<(), SlurmError> {
        self.update(names, slurm_sys::NODE_STATE_UNDRAIN, None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_hostlist_expressions() {
        assert_eq!(
            expand_hostlist("node[01-03],gpu7").unwrap(),
            ["node01", "node02", "node03", "gpu7"]
        );
    }

    #[test]
    fn hostlist_expansion_keeps_input_order() {
        assert_eq!(
            expand_hostlist("node2,gpu7,node1").unwrap(),
            ["node2", "gpu7", "node1"]
        );
    }
}
