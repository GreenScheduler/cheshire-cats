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
use jiff::tz::{AmbiguousOffset, Offset, TimeZone};
use jiff::{SignedDuration, Timestamp, civil};
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
    /// time as "YYYY-MM-DD HH:MM[:SS]". A local time that a daylight saving
    /// time change skips or repeats needs an offset.
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
    parse_time_in( time_string, &TimeZone::system() )
}

/// Parses a time, reading one without an offset as local time in `tz`.
///
/// A local time that a daylight saving time change skips (spring forward) or
/// repeats (fall back) is an error rather than a guess: the caller has to give
/// an offset. Once parsed, a time is an absolute instant, so DST changes no
/// longer affect waits, leases or the window length.
fn parse_time_in(time_string: &str, tz: &TimeZone) -> Result<Timestamp, String> {
    if time_string == "now" {
        return Ok( Timestamp::now() );
    }
    if let Ok(ts) = time_string.parse::<Timestamp>() {
        return Ok(ts);
    }
    let local: civil::DateTime = time_string.parse().map_err( |e| format!("{e}") )?;
    let with_offset            = |offset: Offset| {
        offset
            .to_timestamp(local)
            .map( |ts| ts.display_with_offset(offset).to_string() )
            .unwrap_or_default()
    };
    match tz.to_ambiguous_timestamp(local).offset() {
        AmbiguousOffset::Unambiguous { offset } => {
            offset.to_timestamp(local).map_err( |e| e.to_string() )
        }
        AmbiguousOffset::Gap { .. } => Err( format!(
            "{local} does not exist in the local time zone: clocks skip it \
             for daylight saving time. Pick another time or give an offset"
        ) ),
        AmbiguousOffset::Fold { before, after } => Err( format!(
            "{local} occurs twice in the local time zone: clocks repeat it \
             for daylight saving time. Give an offset: {} (first) or {} (second)",
            with_offset(before),
            with_offset(after)
        ) ),
    }
}

fn local(time_stamp: Timestamp) -> impl std::fmt::Display {
    local_in( time_stamp, TimeZone::system() )
}

/// The zone abbreviation tells apart the two passes through a repeated hour.
fn local_in(time_stamp: Timestamp, tz: TimeZone) -> impl std::fmt::Display {
    time_stamp
        .to_zoned(tz)
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
        "{} of {} nodes known to slurmctld; drain at {}, release at {}; lease {lease} s, renewed every {} s",
        gate.waiting(),
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

    // Daylight saving time. A fixed POSIX rule, not the tz database, so the
    // tests don't depend on the machine's zone or installed zoneinfo.
    // In 2026 New York springs forward on March 8 (02:00 EST -> 03:00 EDT)
    // and falls back on November 1 (02:00 EDT -> 01:00 EST).

    fn new_york() -> TimeZone {
        TimeZone::posix("EST5EDT,M3.2.0,M11.1.0").unwrap()
    }

    fn at(time_string: &str) -> Timestamp {
        parse_time_in( time_string, &new_york() ).unwrap()
    }

    fn hours_between(from: &str, to: &str) -> i64 {
        at(from).duration_until( at(to) ).as_secs() / 3600
    }

    #[test]
    fn rejects_local_time_skipped_by_spring_forward() {
        let err = parse_time_in( "2026-03-08 02:30", &new_york() ).unwrap_err();
        assert!(err.contains("does not exist"), "{err}");
    }

    #[test]
    fn rejects_local_time_repeated_by_fall_back_and_suggests_offsets() {
        let err = parse_time_in( "2026-11-01 01:30", &new_york() ).unwrap_err();
        assert!(err.contains("occurs twice"), "{err}");
        assert!(err.contains("2026-11-01T01:30:00-04:00"), "{err}");
        assert!(err.contains("2026-11-01T01:30:00-05:00"), "{err}");
    }

    #[test]
    fn offset_picks_either_pass_through_a_repeated_hour() {
        assert_eq!( at("2026-11-01T01:30:00-04:00"), at("2026-11-01T05:30:00Z") );
        assert_eq!( at("2026-11-01T01:30:00-05:00"), at("2026-11-01T06:30:00Z") );
    }

    #[test]
    fn accepts_local_times_at_the_edges_of_a_dst_change() {
        assert_eq!( at("2026-03-08 01:59"), at("2026-03-08T06:59:00Z") );
        assert_eq!( at("2026-03-08 03:00"), at("2026-03-08T07:00:00Z") );
        assert_eq!( at("2026-11-01 00:59"), at("2026-11-01T04:59:00Z") );
        assert_eq!( at("2026-11-01 02:00"), at("2026-11-01T07:00:00Z") );
    }

    /// Before the window: the wait until the drain starts is real elapsed
    /// time, not the difference between wall-clock readings.
    #[test]
    fn wait_for_drain_start_across_dst_change_is_real_time() {
        assert_eq!( hours_between("2026-03-07 12:00", "2026-03-08 12:00"), 23 );
        assert_eq!( hours_between("2026-10-31 12:00", "2026-11-01 12:00"), 25 );
    }

    /// During the window: its length, and so the number of lease renewals,
    /// is real elapsed time.
    #[test]
    fn window_across_dst_change_is_real_time() {
        assert_eq!( hours_between("2026-03-07 22:00", "2026-03-08 04:00"), 5 );
        assert_eq!( hours_between("2026-10-31 22:00", "2026-11-01 04:00"), 7 );
    }

    /// A lease is relative seconds (slurmctld stores now + lease as Unix
    /// time), so an hour-long lease across the change is an hour, and the
    /// local time we log names the right side of it.
    #[test]
    fn lease_across_dst_change_is_real_time() {
        let hour = Duration::from_secs(3600);

        let start = at("2026-03-08 01:30");
        assert_eq!( local_in( start, new_york() ).to_string(), "2026-03-08 01:30 EST" );
        assert_eq!( local_in( start + hour, new_york() ).to_string(), "2026-03-08 03:30 EDT" );

        let start = at("2026-11-01T01:30:00-04:00");
        assert_eq!( local_in( start, new_york() ).to_string(), "2026-11-01 01:30 EDT" );
        assert_eq!( local_in( start + hour, new_york() ).to_string(), "2026-11-01 01:30 EST" );
    }
}
