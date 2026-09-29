//! What the daemon knows about a node, and how it reads Slurm's state word.

use std::fmt;

/// Every drain reason we write starts with this. A node is ours only while
/// it is drained with a reason carrying this prefix.
pub const REASON_PREFIX: &str = "cheshire-cats:";

/// Slurm's node state: a 4-bit base state plus flag bits (defined in `slurm.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeState(pub u32);

impl NodeState {
    fn base(self) -> u32 {
        self.0 & slurm_sys::NODE_STATE_BASE
    }

    fn has(self, flag: u32) -> bool {
        self.0 & flag != 0
    }

    pub fn is_drain(self) -> bool {
        self.has(slurm_sys::NODE_STATE_DRAIN)
    }

    pub fn is_down(self) -> bool {
        self.base() == slurm_sys::NODE_STATE_DOWN
    }

    pub fn is_fail(self) -> bool {
        self.has(slurm_sys::NODE_STATE_FAIL)
    }
   
    /// Jobs are still running on the node or cleaning up after themselves.
    pub fn is_busy(self) -> bool {
        matches!(
            self.base(),
            slurm_sys::NODE_STATE_ALLOCATED | slurm_sys::NODE_STATE_MIXED
        ) || self.has(slurm_sys::NODE_STATE_COMPLETING)
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let base = match self.base() {
            slurm_sys::NODE_STATE_UNKNOWN   => "UNKNOWN",
            slurm_sys::NODE_STATE_DOWN      => "DOWN",
            slurm_sys::NODE_STATE_IDLE      => "IDLE",
            slurm_sys::NODE_STATE_ALLOCATED => "ALLOCATED",
            slurm_sys::NODE_STATE_MIXED     => "MIXED",
            slurm_sys::NODE_STATE_FUTURE    => "FUTURE",
            _ => "OTHER",
        };
        f.write_str(base)?;
        for (flag, name) in [
            (slurm_sys::NODE_STATE_DRAIN,      "DRAIN"),
            (slurm_sys::NODE_STATE_FAIL,       "FAIL"),
            (slurm_sys::NODE_STATE_COMPLETING, "COMPLETING"),
        ] {
            if self.has(flag) {
                write!(f, "+{name}")?;
            }
        }
        Ok( () )
    }
}

/// Whether a drained node still has jobs to finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Draining,
    Drained,
}

/// The parts of `node_info_t` the daemon looks at.
#[derive(Clone, Debug)]
pub struct NodeView {
    pub name:   String,
    pub state:  NodeState,
    pub reason: Option<String>,
    /// Absolute Unix time of a pending automatic resume, 0 if none is armed.
    pub resume_after: i64,
}

impl NodeView {
    fn reason_is_ours(&self) -> bool {
        self.reason
            .as_deref()
            .is_some_and(|r| r.starts_with(REASON_PREFIX))
    }

    /// Drained by us (this run or an earlier one).
    pub fn is_ours(&self) -> bool {
        self.state.is_drain() && self.reason_is_ours()
    }

    /// Taken out of service by someone else: not ours to touch.
    pub fn is_foreign(&self) -> bool {
        ( self.state.is_drain() || self.state.is_down() || self.state.is_fail() )
            && !self.reason_is_ours()
    }

    pub fn phase(&self) -> Phase {
        if self.state.is_busy() {
            Phase::Draining
        } else {
            Phase::Drained
        }
    }

    /// State and reason, for log lines.
    pub fn describe(&self) -> String {
        match &self.reason {
            Some(r) => format!("{}, reason \"{r}\"", self.state),
            None    => self.state.to_string(),
        }
    }
}
