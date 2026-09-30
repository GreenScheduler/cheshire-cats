//! The drain window for a fixed set of nodes.
//!
//! [`Gate`] has no clock and never sleeps: the daemon loop calls [`Gate::tick`]
//! every interval while the window is open and [`Gate::release`] when it
//! closes. All Slurm access goes through [`Controller`], so the rules below
//! are tested against a fake slurmctld.
//!
//! Rules:
//! - A node is ours only while it is drained with a reason starting with
//!   [`REASON_PREFIX`]. We never drain over, undrain or otherwise alter a node
//!   someone else has drained or shut off, or if a node failed.
//! - Once someone else acts on a node we hold (undrain, resume, re-drain with
//!   their own reason), we give up for the rest of the window and do not
//!   drain it again.
//! - Every drain carries a lease (`resume_after`). If the daemon dies,
//!   slurmctld resumes the node itself when the lease runs out: the gate
//!   fails open.
//! - The state read back from slurmctld, not the return code of an update,
//!   decides what we hold.
//!
//! Nodes are kept in input order (duplicates dropped), and every pass, log
//! sequence, and node list sent to slurmctld follows that order.

use std::collections::{HashMap, HashSet};

use log::{error, info, warn};

use crate::node::{NodeView, Phase, REASON_PREFIX};
use crate::slurm::SlurmError;

/// Timer to set on a drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lease {
    /// slurmctld resumes the node this many seconds from now.
    Seconds(u32),
    /// Remove any pending automatic resume.
    Cancel,
}

/// The slurmctld operations the gate needs.
pub trait Controller {
    /// Current view of the named nodes. Names slurmctld does not know are
    /// absent from the result.
    fn load(&mut self, names: &[&str]) -> Result<Vec<NodeView>, SlurmError>;
    /// Drain, or re-drain an already-drained node. `reason: None` leaves the
    /// stored reason unchanged.
    fn drain(
        &mut self,
        names:  &[&str],
        reason: Option<&str>,
        lease:  Lease,
    ) -> Result<(), SlurmError>;
    fn undrain(&mut self, names: &[&str]) -> Result<(), SlurmError>;
}

/// Holds the state of our interaction with a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Not drained yet: the window has not opened, or the drain request
    /// failed and will be retried on the next tick.
    Waiting,
    /// Drained by us. `lease_until` is the resume time slurmctld stored at
    /// our last renewal (0 if it did not arm one).
    Held { lease_until: i64, phase: Phase },
    /// Left alone: someone else was interacting with the node when the window opened,
    /// unknown to slurmctld, or the drain did not take.
    Skipped,
    /// Someone else acted on it while we held it.
    SteppedAside,
    /// Undrained by us when the window closed.
    Released,
}

#[derive(Debug)]
pub enum GateError {
    Slurm(SlurmError),
    UnknownNodes(Vec<String>),
}

impl std::fmt::Display for GateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Slurm(e)            => e.fmt(f),
            Self::UnknownNodes(names) => {
                write!( f, "unknown to slurmctld: {}", names.join(",") )
            }
        }
    }
}

impl std::error::Error for GateError {}

impl From<SlurmError> for GateError {
    fn from(e: SlurmError) -> Self {
        Self::Slurm(e)
    }
}

/// A node in the gate and what we are doing with it.
struct GatedNode {
    name:   String,
    status: Status,
}

/// The nodes we influence during one drain window.
/// Currently, the influence is only `DRAIN`, undone with `UNDRAIN` when the
/// window closes. Nodes someone else has already taken out of service are
/// skipped, and nodes someone else acts on while we hold them are given up
/// for the rest of the window (see the module docs for the ownership rules).
pub struct Gate {
    /// One entry per unique node, in input order.
    nodes: Vec<GatedNode>,
    /// Full reason stored on each node, starting with `REASON_PREFIX`.
    reason: String,
    /// Seconds after our last renewal at which slurmctld resumes a node on
    /// its own. Renewed on every tick.
    lease: u32,
}

impl Gate {
    /// Keeps `names` in the order given, dropping repeats after the first.
    /// `reason` is stored on each node as "`REASON_PREFIX` `reason`".
    /// `lease` is in seconds and must outlast at least two ticks.
    pub fn new(names: impl IntoIterator<Item = String>, reason: &str, lease: u32) -> Self {
        let mut seen = HashSet::new();
        let nodes    = names
            .into_iter()
            .filter( |n| seen.insert( n.clone() ) )
            .map( |name| GatedNode { name, status: Status::Waiting } )
            .collect();
        Self {
            nodes,
            reason: format!("{REASON_PREFIX} {reason}"),
            lease,
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Node names, in input order.
    fn names(&self) -> Vec<&str> {
        self.nodes.iter().map( |n| n.name.as_str() ).collect()
    }

    fn count(&self, pred: impl Fn(&Status) -> bool) -> usize {
        self.nodes.iter().filter( |n| pred(&n.status) ).count()
    }

    pub fn waiting(&self) -> usize {
        self.count(|s| *s == Status::Waiting)
    }

    pub fn held(&self) -> usize {
        self.count( |s| matches!(s, Status::Held { .. }) )
    }

    /// Checks that slurmctld knows every node, before committing to a window.
    pub fn validate(&self, ctl: &mut impl Controller) -> Result<(), GateError> {
        let found: HashSet<String> = ctl.load( &self.names() )?.into_iter().map(|v| v.name).collect();
        let missing: Vec<String>   = self
            .nodes
            .iter()
            .filter( |n| !found.contains(&n.name) )
            .map( |n| n.name.clone() )
            .collect();
        if missing.is_empty() {
            Ok( () )
        } else {
            Err( GateError::UnknownNodes(missing) )
        }
    }

    fn load_map(&self, ctl: &mut impl Controller) -> Result<HashMap<String, NodeView>, SlurmError> {
        Ok(
            ctl
            .load( &self.names() )?
            .into_iter()
            .map( |v| (v.name.clone(), v) )
            .collect()
        )
    }

    /// One pass while the window is open: drain nodes still waiting, check
    /// that the nodes we hold are still ours, and renew their lease.
    /// Three RPCs regardless of node count: poll, drain/renew, read back.
    pub fn tick(&mut self, ctl: &mut impl Controller) -> Result<(), SlurmError> {
        let now = self.load_map(ctl)?;
        // Indices into `nodes`, so the drain list keeps input order.
        let mut to_drain = Vec::new();

        for (i, node) in self.nodes.iter_mut().enumerate() {
            let (name, status) = (&node.name, &mut node.status);
            let view           = now.get(name);
            match *status {
                Status::Waiting => match view {
                    None => {
                        warn!("{name}: unknown to slurmctld; skipping");
                        *status = Status::Skipped;
                    }
                    Some(v) if v.is_foreign() => {
                        info!(
                            "{name}: already out of service ({}); leaving it alone",
                            v.describe()
                        );
                        *status = Status::Skipped;
                    }
                    Some(v) => {
                        if v.is_ours() {
                            info!("{name}: adopting drain left by an earlier run");
                        }
                        to_drain.push(i);
                    }
                },
                Status::Held { lease_until, phase } => {
                    if let Some(v) = view.filter( |v| v.is_ours() ) {
                        if v.phase() != phase {
                            info!( "{name}: {}", describe_phase( v.phase() ) );
                        }
                        *status = Status::Held {
                            lease_until,
                            phase: v.phase(),
                        };
                        to_drain.push(i);
                    } else {
                        step_aside(ctl, name, view, lease_until);
                        *status = Status::SteppedAside;
                    }
                }
                Status::Skipped | Status::SteppedAside | Status::Released => {}
            }
        }

        if to_drain.is_empty() {
            return Ok( () );
        }
        let refs: Vec<&str> = to_drain.iter().map( |&i| self.nodes[i].name.as_str() ).collect();
        let drain_ok        = match ctl.drain( &refs, Some(&self.reason), Lease::Seconds(self.lease) ) {
            Ok( () ) => true,
            Err(e)   => {
                error!( "drain {}: {e}", refs.join(",") );
                false
            }
        };

        // Read back the stored resume times: they tell us later whether a
        // pending resume on a node someone took over is still ours.
        let back = self.load_map(ctl)?;
        for &i in &to_drain {
            let node           = &mut self.nodes[i];
            let (name, status) = (&node.name, &mut node.status);
            match (back.get(name).filter( |v| v.is_ours() ), *status) {
                (Some(v), Status::Waiting) => {
                    info!(
                        "{name}: drained, no new jobs will start; {}",
                        describe_phase( v.phase() )
                    );
                    if v.resume_after == 0 {
                        warn!("{name}: slurmctld did not arm the lease");
                    }
                    *status = Status::Held {
                        lease_until: v.resume_after,
                        phase: v.phase(),
                    };
                }
                (Some(v), Status::Held { phase, .. }) => {
                    *status = Status::Held {
                        lease_until: v.resume_after,
                        phase,
                    };
                }
                // Changed between renewal and read-back; the next tick sees it.
                (None, Status::Held { .. }) => {}
                (None, Status::Waiting) if drain_ok => {
                    warn!("{name}: drain did not take; leaving it alone");
                    *status = Status::Skipped;
                }
                // The request failed: retry on the next tick.
                _ => {}
            }
        }
        Ok( () )
    }

    /// Closes the window: undrains every node we still hold.
    pub fn release(&mut self, ctl: &mut impl Controller) -> Result<(), SlurmError> {
        let now            = self.load_map(ctl)?;
        let mut to_undrain = Vec::new();
        for (i, node) in self.nodes.iter_mut().enumerate() {
            let (name, status) = (&node.name, &mut node.status);
            let Status::Held { lease_until, .. } = *status else {
                continue;
            };
            let view = now.get(name);
            if view.is_some_and(NodeView::is_ours) {
                to_undrain.push(i);
            } else {
                step_aside(ctl, name, view, lease_until);
                *status = Status::SteppedAside;
            }
        }
        if to_undrain.is_empty() {
            return Ok( () );
        }

        let refs: Vec<&str> = to_undrain.iter().map( |&i| self.nodes[i].name.as_str() ).collect();
        if let Err(e) = ctl.undrain(&refs) {
            error!( "undrain {}: {e}", refs.join(",") );
        }
        let back = self.load_map(ctl)?;
        for i in to_undrain {
            let node = &mut self.nodes[i];
            let name = &node.name;
            if back.get(name).is_some_and(NodeView::is_ours) {
                error!("{name}: still drained; the lease will release it");
            } else {
                info!("{name}: released");
                node.status = Status::Released;
            }
        }
        Ok( () )
    }
}

fn describe_phase(phase: Phase) -> &'static str {
    match phase {
        Phase::Draining => "draining, waiting for running jobs to finish",
        Phase::Drained  => "drained, no jobs running",
    }
}

/// Someone else now owns a node we held. If they re-drained it without
/// setting their own `resume_after`, our timer is still armed and would
/// resume *their* drain when it expires, so cancel it. Re-sending no reason
/// keeps the request equivalent and leaves everything else as they set it.
fn step_aside(ctl: &mut impl Controller, name: &str, view: Option<&NodeView>, lease_until: i64) {
    let Some(v) = view else {
        warn!("{name}: no longer known to slurmctld; stepping aside");
        return;
    };
    if !v.state.is_drain() {
        // An undrain or resume also cleared our lease.
        info!(
            "{name}: drain removed by someone else ({}); stepping aside",
            v.describe()
        );
        return;
    }
    info!(
        "{name}: drain taken over ({}); stepping aside",
        v.describe()
    );
    if v.resume_after != 0 && v.resume_after == lease_until {
        match ctl.drain(&[name], None, Lease::Cancel) {
            Ok( () ) => info!("{name}: canceled our pending resume"),
            Err(e)   => error!("{name}: could not cancel our pending resume: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeState;
    use slurm_sys as sys;
    use std::collections::BTreeMap;

    const IDLE: u32      = sys::NODE_STATE_IDLE;
    const ALLOCATED: u32 = sys::NODE_STATE_ALLOCATED;
    const DOWN: u32      = sys::NODE_STATE_DOWN;
    const DRAIN: u32     = sys::NODE_STATE_DRAIN;
    const LEASE: u32     = 90;

    struct FakeNode {
        state: u32,
        reason: Option<String>,
        resume_after: i64,
    }

    /// Mimics the slurmctld behavior the gate relies on: DRAIN on a drained
    /// node only moves the timer (and reason, if given); UNDRAIN clears DRAIN
    /// and the timer.
    #[derive(Default)]
    struct Fake {
        nodes: BTreeMap<String, FakeNode>,
        now: i64,
        fail_drain: bool,
        drain_calls: Vec<(Vec<String>, Option<String>, Lease)>,
    }

    impl Fake {
        fn with(nodes: &[(&str, u32, Option<&str>)]) -> Self {
            let mut f = Self {
                now: 1000,
                ..Default::default()
            };
            for &(name, state, reason) in nodes {
                f.nodes.insert(
                    name.into(),
                    FakeNode {
                        state,
                        reason: reason.map(Into::into),
                        resume_after: 0,
                    },
                );
            }
            f
        }

        fn node(&self, name: &str) -> &FakeNode {
            &self.nodes[name]
        }

        /// An admin runs `scontrol update state=drain reason=...`.
        fn admin_drain(&mut self, name: &str, reason: &str, resume_after: Option<i64>) {
            let n = self.nodes.get_mut(name).unwrap();
            n.state |= DRAIN;
            n.reason = Some( reason.into() );
            if let Some(t) = resume_after {
                n.resume_after = t;
            }
        }

        /// An admin runs `scontrol update state=resume`.
        fn admin_resume(&mut self, name: &str) {
            let n = self.nodes.get_mut(name).unwrap();
            n.state &= !DRAIN;
            n.reason = None;
            n.resume_after = 0;
        }
    }

    impl Controller for Fake {
        fn load(&mut self, names: &[&str]) -> Result<Vec<NodeView>, SlurmError> {
            Ok(
                self
                .nodes
                .iter()
                .filter( |(name, _)| names.contains( &name.as_str() ) )
                .map(|(name, n)| NodeView {
                    name: name.clone(),
                    state: NodeState(n.state),
                    reason: n.reason.clone(),
                    resume_after: n.resume_after,
                })
                .collect() 
            )
        }

        fn drain(
            &mut self,
            names: &[&str],
            reason: Option<&str>,
            lease: Lease,
        ) -> Result<(), SlurmError> {
            self.drain_calls.push( (
                names.iter().map( |s| s.to_string() ).collect(),
                reason.map(Into::into),
                lease,
            ) );
            if self.fail_drain {
                return Err( SlurmError::for_test("update nodes") );
            }
            for name in names {
                let n = self.nodes.get_mut(*name).unwrap();
                n.state |= DRAIN;
                if let Some(r) = reason {
                    n.reason = Some( r.into() );
                }
                n.resume_after = match lease {
                    Lease::Seconds(s) => self.now + s as i64,
                    Lease::Cancel => 0,
                };
            }
            Ok( () )
        }

        fn undrain(&mut self, names: &[&str]) -> Result<(), SlurmError> {
            for name in names {
                let n = self.nodes.get_mut(*name).unwrap();
                n.state &= !DRAIN;
                n.resume_after = 0;
            }
            Ok( () )
        }
    }

    fn gate(names: &[&str]) -> Gate {
        Gate::new(names.iter().map( |s| s.to_string() ), "test", LEASE)
    }

    fn status(g: &Gate, name: &str) -> Status {
        g.nodes.iter().find(|n| n.name == name).unwrap().status
    }

    fn held(lease_until: i64, phase: Phase) -> Status {
        Status::Held { lease_until, phase }
    }

    #[test]
    fn keeps_input_order_and_drops_repeats() {
        let mut ctl = Fake::with(&[("n1", IDLE, None), ("n2", IDLE, None), ("n3", IDLE, None)]);
        let mut g = gate(&["n3", "n1", "n3", "n2", "n1"]);
        assert_eq!(g.names(), ["n3", "n1", "n2"]);
        assert_eq!(g.node_count(), 3);

        g.tick(&mut ctl).unwrap();
        assert_eq!(ctl.drain_calls[0].0, ["n3", "n1", "n2"]);
    }

    #[test]
    fn drains_open_nodes_and_leaves_foreign_ones_alone() {
        let mut ctl = Fake::with(&[
            ("n1", IDLE, None),
            ("n2", ALLOCATED, None),
            ( "n3", IDLE | DRAIN, Some("admin: bad dimm") ),
            ( "n4", DOWN, Some("Not responding") ),
        ]);
        let mut g = gate(&["n1", "n2", "n3", "n4"]);
        g.tick(&mut ctl).unwrap();

        assert_eq!( status(&g, "n1"), held(1090, Phase::Drained) );
        assert_eq!( status(&g, "n2"), held( 1090, Phase::Draining ) );
        assert_eq!(status(&g, "n3"), Status::Skipped);
        assert_eq!(status(&g, "n4"), Status::Skipped);
        assert_eq!(
            ctl.node("n1").reason.as_deref(),
            Some("cheshire-cats: test")
        );
        assert_eq!( ctl.node("n3").reason.as_deref(), Some("admin: bad dimm") );
        assert_eq!(ctl.node("n4").state & DRAIN, 0);
    }

    #[test]
    fn adopts_a_drain_left_by_an_earlier_run() {
        let mut ctl = Fake::with(&[( "n1", IDLE | DRAIN, Some("cheshire-cats: old") )]);
        let mut g = gate(&["n1"]);
        g.tick(&mut ctl).unwrap();
        assert_eq!( status(&g, "n1"), held(1090, Phase::Drained) );
    }

    #[test]
    fn renewal_extends_the_lease_and_tracks_phase() {
        let mut ctl = Fake::with(&[("n1", ALLOCATED, None)]);
        let mut g = gate(&["n1"]);
        g.tick(&mut ctl).unwrap();
        assert_eq!( status(&g, "n1"), held(1090, Phase::Draining) );

        ctl.now = 1030;
        ctl.nodes.get_mut("n1").unwrap().state = IDLE | DRAIN;
        g.tick(&mut ctl).unwrap();
        assert_eq!( status(&g, "n1"), held(1120, Phase::Drained) );
    }

    #[test]
    fn failed_drain_is_retried_on_the_next_tick() {
        let mut ctl = Fake::with(&[("n1", IDLE, None)]);
        ctl.fail_drain = true;
        let mut g = gate(&["n1"]);
        g.tick(&mut ctl).unwrap();
        assert_eq!(status(&g, "n1"), Status::Waiting);

        ctl.fail_drain = false;
        g.tick(&mut ctl).unwrap();
        assert_eq!( status(&g, "n1"), held(1090, Phase::Drained) );
    }

    #[test]
    fn steps_aside_when_an_admin_resumes_the_node() {
        let mut ctl = Fake::with(&[("n1", IDLE, None)]);
        let mut g = gate(&["n1"]);
        g.tick(&mut ctl).unwrap();

        ctl.admin_resume("n1");
        ctl.drain_calls.clear();
        g.tick(&mut ctl).unwrap();
        assert_eq!(status(&g, "n1"), Status::SteppedAside);
        assert!(ctl.drain_calls.is_empty(), "must not re-drain");

        g.release(&mut ctl).unwrap();
        assert_eq!(status(&g, "n1"), Status::SteppedAside);
        assert_eq!(ctl.node("n1").state & DRAIN, 0);
    }

    #[test]
    fn cancels_our_lease_when_an_admin_redrains() {
        let mut ctl = Fake::with(&[("n1", IDLE, None)]);
        let mut g = gate(&["n1"]);
        g.tick(&mut ctl).unwrap();

        // Their drain keeps our timer, which would later resume their node.
        ctl.admin_drain("n1", "admin: fan", None);
        assert_eq!(ctl.node("n1").resume_after, 1090);
        g.tick(&mut ctl).unwrap();

        assert_eq!(status(&g, "n1"), Status::SteppedAside);
        let n = ctl.node("n1");
        assert_eq!(n.resume_after, 0);
        assert_eq!( n.reason.as_deref(), Some("admin: fan") );
        assert_ne!(n.state & DRAIN, 0);
    }

    #[test]
    fn keeps_a_resume_time_the_admin_set() {
        let mut ctl = Fake::with(&[("n1", IDLE, None)]);
        let mut g = gate(&["n1"]);
        g.tick(&mut ctl).unwrap();

        ctl.admin_drain( "n1", "admin: fan", Some(5000) );
        g.tick(&mut ctl).unwrap();
        assert_eq!(status(&g, "n1"), Status::SteppedAside);
        assert_eq!(ctl.node("n1").resume_after, 5000);
    }

    #[test]
    fn release_undrains_only_nodes_still_ours() {
        let mut ctl = Fake::with(&[("n1", IDLE, None), ("n2", IDLE, None)]);
        let mut g = gate(&["n1", "n2"]);
        g.tick(&mut ctl).unwrap();

        ctl.admin_drain( "n2", "admin: reboot", Some(5000) );
        g.release(&mut ctl).unwrap();

        assert_eq!(status(&g, "n1"), Status::Released);
        assert_eq!(ctl.node("n1").state & DRAIN, 0);
        assert_eq!(ctl.node("n1").resume_after, 0);
        assert_eq!(status(&g, "n2"), Status::SteppedAside);
        assert_ne!(ctl.node("n2").state & DRAIN, 0);
    }

    #[test]
    fn validate_reports_unknown_nodes() {
        let mut ctl = Fake::with(&[("n1", IDLE, None)]);
        match gate(&["n9", "n1", "n8"]).validate(&mut ctl) {
            Err( GateError::UnknownNodes(missing) ) => assert_eq!(missing, ["n9", "n8"]),
            other => panic!("expected UnknownNodes, got {other:?}"),
        }
    }
}
