//! cheshire-cats: drains a set of Slurm nodes for a time window, then releases them.
//! Jobs already running finish; no new ones start.
//! If an admin acts on one of the nodes during the window, the daemon steps aside
//! for that node.
//!
//! Must run as root or SlurmUser: slurmctld refuses node updates from
//! anyone else.

mod gate;
mod node;
mod slurm;

use std::error::Error;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use clap::Parser;
use jiff::{SignedDuration, Timestamp, civil, tz::TimeZone};
use log::{error, info, warn};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

use gate::Gate;
use slurm::Slurm;

/// Drain Slurm nodes for a time window, then let them accept jobs again.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Nodes to drain, as Slurm hostlist expressions (e.g. node[01-04]).
    #[arg(required = true)]
    nodes: Vec<String>,

    /// When to start draining: "now", RFC 3339 with an offset, or local
    /// time as "YYYY-MM-DD HH:MM[:SS]".
    #[arg(long, default_value = "now", value_parser = parse_time)]
    drain_at: Timestamp,

    /// When the nodes may accept jobs again (same formats as --drain-at).
    #[arg(long, value_parser = parse_time)]
    release_at: Timestamp,

    /// Seconds between checks and lease renewals.
    #[arg(
        short, long, default_value_t = 30,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    interval: u32,

    /// Seconds after the last renewal at which slurmctld releases the nodes
    /// on its own if the daemon has died [default: 3 x interval].
    #[arg(short, long)]
    lease: Option<u32>,

    /// Reason text, stored on each node as "cheshire-cats: <text> until <release time>".
    #[arg(short, long, default_value = "carbon-aware drain")]
    reason: String,

    /// slurm.conf to use instead of $SLURM_CONF or the built-in default.
    #[arg(long)]
    slurm_conf: Option<PathBuf>,
}

fn parse_time(time_string: &str) -> Result<Timestamp, String> {
    if time_string == "now" {
        return Ok( Timestamp::now() );
    }
    if let Ok(ts) = time_string.parse::<Timestamp>() {
        return Ok(ts);
    }
    let local: civil::DateTime = time_string.parse().map_err( |e| format!("{e}") )?;
    local
        .to_zoned( TimeZone::system() )
        .map( |z| z.timestamp() )
        .map_err( |e| e.to_string() )
}

fn local(time_stamp: Timestamp) -> impl std::fmt::Display {
    time_stamp
        .to_zoned( TimeZone::system() )
        .strftime("%Y-%m-%d %H:%M %Z")
}

/// Delivers SIGINT/SIGTERM on a channel so sleep can be interrupted.
fn watch_signals() -> std::io::Result<Receiver<i32>> {
    let mut signals                  = Signals::new([SIGINT, SIGTERM])?;
    let (sender_chnl, receiver_chnl) = mpsc::channel();
    std::thread::spawn(move || {
        for sig in signals.forever() {
            if sender_chnl.send(sig).is_err() {
                break;
            }
        }
    });
    Ok(receiver_chnl)
}

/// Sleeps until `deadline`; returns true if a signal arrived first. Sleeps
/// at most a minute at a time so wall-clock jumps (NTP, suspend) are noticed.
fn wait_until(deadline: Timestamp, signals: &Receiver<i32>) -> bool {
    loop {
        let time_left = Timestamp::now().duration_until(deadline);
        if time_left <= SignedDuration::ZERO {
            return false;
        }
        let chunk = Duration::try_from(time_left)
            .unwrap_or_default()
            .min( Duration::from_secs(60) );
        match signals.recv_timeout(chunk) {
            Ok(_) => return true,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => std::thread::sleep(chunk),
        }
    }
}

fn run(args: Args) -> Result<ExitCode, Box<dyn Error>> {
    let lease = args.lease.unwrap_or( args.interval.saturating_mul(3) );
    if lease < args.interval.saturating_mul(2) {
        return Err(
            format!(
                "lease ({lease} s) must be at least twice the interval ({} s), \
                 or one slow poll releases the nodes",
                args.interval
            )
            .into() 
        );
    }
    if args.release_at <= args.drain_at {
        return Err( "--release-at must be later than --drain-at".into() );
    }
    if args.release_at <= Timestamp::now() {
        return Err( "--release-at is in the past".into() );
    }

    // Input order matters; Gate::new drops repeated names.
    let mut names = Vec::new();
    for expr in &args.nodes {
        names.extend(slurm::expand_hostlist(expr)?);
    }
    if names.is_empty() {
        return Err( "no node names given".into() );
    }

    let signals  = watch_signals()?;
    let mut ctl  = Slurm::init( args.slurm_conf.as_deref() )?;
    let reason   = format!( "{} until {}", args.reason, local(args.release_at) );
    let mut gate = Gate::new(names, &reason, lease);
    gate.validate(&mut ctl)?;

    let interval = Duration::from_secs( args.interval.into() );
    info!(
        "{} nodes; drain at {}, release at {}; lease {lease} s, renewed every {} s",
        gate.node_count(),
        local(args.drain_at),
        local(args.release_at),
        args.interval
    );

    if wait_until(args.drain_at, &signals) {
        info!("signal received before the drain window; nothing to undo");
        return Ok(ExitCode::SUCCESS);
    }

    loop {
        if let Err(e) = gate.tick(&mut ctl) {
            warn!("{e}; retrying in {} s", args.interval);
        }
        if gate.held() == 0 && gate.waiting() == 0 {
            info!("no nodes left to hold");
            break;
        }
        let next = (Timestamp::now() + interval).min(args.release_at);
        if wait_until(next, &signals) {
            info!("signal received; releasing early");
            break;
        }
        if Timestamp::now() >= args.release_at {
            break;
        }
    }

    // Keep trying until our leases would have run out anyway.
    let give_up = Timestamp::now() + Duration::from_secs( lease.into() );
    while let Err(e) = gate.release(&mut ctl) {
        warn!("{e}");
        let now = Timestamp::now();
        if now >= give_up || wait_until( (now + interval).min(give_up), &signals ) {
            break;
        }
    }

    let (held, waiting) = ( gate.held(), gate.waiting() );
    if held > 0 {
        error!("{held} nodes not released; slurmctld will resume them when their lease runs out");
    }
    if waiting > 0 {
        error!("{waiting} nodes were never drained");
    }
    Ok(if held + waiting > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();
    match run( Args::parse() ) {
        Ok(code) => code,
        Err(e)   => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rfc3339_and_rejects_garbage() {
        assert_eq!(
            parse_time("2026-09-29T18:00:00+01:00").unwrap(),
            "2026-09-29T17:00:00Z".parse::<Timestamp>().unwrap()
        );
        assert!( parse_time("tomorrow-ish").is_err() );
    }

    #[test]
    fn parses_local_time_without_offset() {
        let expected = civil::datetime(2026, 9, 29, 18, 0, 0, 0)
            .to_zoned( TimeZone::system() )
            .unwrap()
            .timestamp();
        assert_eq!(parse_time("2026-09-29 18:00").unwrap(), expected);
        assert_eq!(parse_time("2026-09-29T18:00").unwrap(), expected);
    }
}
