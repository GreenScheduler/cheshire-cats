//! End-to-end tests against the Docker dev cluster (`docker/compose.yml`).
//!
//! They run the real daemon binary as a child process against a real
//! slurmctld, submit jobs, act as an admin, and send signals.
//! Thus, they only work inside the `cheshire` container, where libslurm,
//! munge, Slurm's command-line tools and root access are all available.
//!
//! Run them from the host with
//!
//! ```sh
//! docker/cluster-test.sh
//! ```
//!
//! which starts the cluster, runs integration tests inside `cheshire`, and tears the cluster
//! down again. With a cluster already up (`KEEP_CLUSTER=1`), run the tests directly:
//! `docker exec cheshire cargo test --features cluster-tests --test cluster`.
//!
//! The tests cancel every root job and drain and resume nodes, so they refuse to
//! run anywhere but the dev cluster: `CHESHIRE_DEV_CLUSTER=1` (set only by
//! `docker/compose.yml`), a responding slurmctld, cluster `linux`, and nodes
//! exactly `c1` and `c2` (one CPU each).
//!
//! If the dev cluster is not there, the scenarios are reported as ignored, with the reason;
//! however if `CHESHIRE_REQUIRE_CLUSTER=1` (for CI) they fail instead.
//!
//! The daemon runs with a 5 s interval, so its lease is 15 s; slurmctld checks
//! lease expiry every 30 s. The scenarios share the two nodes, so they run one
//! at a time, and each starts and ends by resetting the cluster. A full run
//! takes about 5 minutes. Daemon logs are kept in
//! `$TMPDIR/cheshire-cluster-tests/`.

use std::fs::{self, File};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use jiff::Timestamp;
use libtest_mimic::{Arguments, Failed, Trial};

const BIN: &str   = env!("CARGO_BIN_EXE_cheshire-cats");
const NODES: &str = "c[1-2]";

type Scenario = fn() -> Result<(), Failed>;

fn main() {
    let mut args = Arguments::from_args();
    // All scenarios drain and resume the same two nodes.
    args.test_threads = Some(1);

    let scenarios: [(&str, Scenario); 4] = [
        ("a_normal_window",            a_normal_window),
        ("b_foreign_drain_and_sigterm", b_foreign_drain_and_sigterm),
        ("c_admin_acts_on_held_nodes", c_admin_acts_on_held_nodes),
        ("d_crash_adoption_fail_open", d_crash_adoption_fail_open),
    ];
    let unavailable = dev_cluster().err();
    let required    = std::env::var("CHESHIRE_REQUIRE_CLUSTER").is_ok_and(|v| v == "1");
    if let Some(why) = &unavailable {
        println!("\ndev cluster not available: {why}");
        if !required {
            println!("cluster scenarios are ignored (set CHESHIRE_REQUIRE_CLUSTER=1 to fail instead)");
        }
    }

    let trials = scenarios
        .into_iter()
        .map( |(name, scenario)| match &unavailable {
            None      => Trial::test(name, scenario),
            Some(why) => {
                let why = why.clone();
                Trial::test( name, move || Err( format!("dev cluster not available: {why}").into() ) )
                    .with_ignored_flag(!required)
            }
        })
        .collect();
    libtest_mimic::run(&args, trials).exit();
}

/// See if this is a dev cluster, and report why not if not
fn dev_cluster() -> Result<(), String> {
    if std::env::var("CHESHIRE_DEV_CLUSTER").as_deref() != Ok("1") {
        return Err( "CHESHIRE_DEV_CLUSTER=1 is not set (docker/compose.yml sets it in the cheshire container)".into() );
    }
    if !PathBuf::from("/etc/slurm/slurm.conf").exists() {
        return Err( "no /etc/slurm/slurm.conf".into() );
    }
    let ping = sh("scontrol", &["ping"]);
    if !ping.contains("is UP") {
        return Err( format!("slurmctld not responding (scontrol ping: {ping:?})") );
    }
    let config  = sh("scontrol", &["show", "config"]);
    let cluster = config
        .lines()
        .find_map( |l| l.strip_prefix("ClusterName") )
        .map( |v| v.trim().trim_start_matches('=').trim() )
        .unwrap_or_default();
    if cluster != "linux" {
        return Err( format!("cluster is {cluster:?}, not the dev cluster \"linux\"") );
    }
    let mut nodes: Vec<String> = sh("sinfo", &["-h", "-N", "-o", "%N"]).lines().map(String::from).collect();
    nodes.sort();
    nodes.dedup();
    if nodes != ["c1", "c2"] {
        return Err( format!("nodes are {nodes:?}, not [\"c1\", \"c2\"]") );
    }
    Ok( () )
}

// Scenarios

/// Normal window with a job running on c1: draining vs drained, reason, lease
/// renewal, new jobs held, phase change, release, the held job then runs.
fn a_normal_window() -> Result<(), Failed> {
    let _clean = Clean::new()?;
    let j1     = sbatch(&["-w", "c1", "--wrap", "sleep 25"])?;
    check( "job 1 running on c1", wait_for( 15, || job_state(&j1) == "RUNNING" ) )?;

    let mut daemon = Daemon::start("A", 60)?;
    pause(6);
    is( "c1 draining (job still running)", state("c1"), "draining" )?;
    is( "c2 drained", state("c2"), "drained" )?;
    starts( "reason is ours", reason("c2"), "cheshire-cats: carbon-aware drain until" )?;
    let ra1 = resume_after("c2");
    check( "lease armed on c2", ra1 != "None" )?;

    let j2 = sbatch(&["--wrap", "hostname"])?;
    pause(2);
    is( "new job pending", job_state(&j2), "PENDING" )?;
    pause(10);
    let ra2 = resume_after("c2");
    check( &format!("lease renewed ({ra1} -> {ra2})"), ra2 > ra1 )?;

    check( "c1 drained after job ended", wait_for( 30, || state("c1") == "drained" ) )?;
    // The daemon notices the phase change at its next tick, up to one interval later.
    daemon.wait_log( "c1: drained, no jobs running", 10 )?;

    let status = daemon.wait_exit(60)?;
    check( &format!("daemon exited 0 ({status})"), status.success() )?;
    daemon.has("c1: released")?;
    daemon.has("c2: released")?;
    is( "lease cleared on release", resume_after("c2"), "None" )?;
    check( "held job ran after release", wait_for( 20, || job_state(&j2).is_empty() ) )?;
    is( "held job completed", sh("sacct", &["-n", "-X", "-j", &j2, "-o", "state"]), "COMPLETED" )?;
    Ok( () )
}

/// A node an admin drained before the window is skipped and left alone;
/// SIGTERM releases the rest early.
fn b_foreign_drain_and_sigterm() -> Result<(), Failed> {
    let _clean = Clean::new()?;
    admin(&["nodename=c1", "state=drain", "reason=admin: maintenance"]);

    let mut daemon = Daemon::start("B", 120)?;
    pause(6);
    daemon.has("c1: already out of service")?;
    is( "c1 keeps admin reason", reason("c1"), "admin: maintenance" )?;
    starts( "c2 drained by us", reason("c2"), "cheshire-cats:" )?;

    daemon.signal(libc::SIGTERM);
    let status = daemon.wait_exit(15)?;
    daemon.has("signal received; releasing early")?;
    check( &format!("daemon exited 0 ({status})"), status.success() )?;
    // UNDRAIN sets NO_RESPOND until slurmd re-registers: "idle*" for a moment.
    check( "c2 idle", wait_for( 10, || state("c2") == "idle" ) )?;
    is( "c1 still admin-drained", reason("c1"), "admin: maintenance" )?;
    Ok( () )
}

/// An admin resumes one held node and re-drains the other: the daemon steps
/// aside on both, cancels its lease on the re-drained one, and, with nothing
/// left to hold, exits on its own.
fn c_admin_acts_on_held_nodes() -> Result<(), Failed> {
    let _clean     = Clean::new()?;
    let mut daemon = Daemon::start("C", 120)?;
    pause(6);
    check( "both drained by us", state("c1") == "drained" && state("c2") == "drained" )?;

    admin(&["nodename=c1", "state=resume"]);
    admin(&["nodename=c2", "state=drain", "reason=admin: fan"]);
    pause(8);
    daemon.has("c1: drain removed by someone else")?;
    daemon.has("c2: drain taken over")?;
    daemon.has("c2: canceled our pending resume")?;
    is( "c2 lease cleared", resume_after("c2"), "None" )?;

    let status = daemon.wait_exit(10)?;
    daemon.has("no nodes left to hold")?;
    check( &format!("daemon exited 0 ({status})"), status.success() )?;
    check( "c1 not re-drained", wait_for( 10, || state("c1") == "idle" ) )?;

    // Well past the lease we canceled, plus slurmctld's 30 s expiry check.
    pause(45);
    is( "c2 still drained", state("c2"), "drained" )?;
    is( "c2 keeps admin reason", reason("c2"), "admin: fan" )?;
    Ok( () )
}

/// The daemon is killed: its drains stay, a new run adopts them, and when that
/// run is killed too, slurmctld resumes the nodes when the lease runs out.
fn d_crash_adoption_fail_open() -> Result<(), Failed> {
    let _clean    = Clean::new()?;
    let mut first = Daemon::start("D1", 600)?;
    pause(6);
    first.kill();
    pause(1);
    check( "nodes still drained after crash", state("c1") == "drained" && state("c2") == "drained" )?;

    let mut second = Daemon::start("D2", 600)?;
    pause(6);
    second.has("c1: adopting drain left by an earlier run")?;
    second.has("c2: adopting drain left by an earlier run")?;
    second.kill();

    let killed = Instant::now();
    let idle   = wait_for( 75, || state("c1") == "idle" && state("c2") == "idle" );
    check( &format!("slurmctld resumed the nodes on lease expiry ({} s after kill)", killed.elapsed().as_secs()), idle )?;
    Ok( () )
}

// The daemon under test

/// The daemon binary, running as a child process with its log in a file.
struct Daemon {
    child: Child,
    log:   PathBuf,
}

impl Daemon {
    /// Starts a window that opens now and closes `release_in` seconds from
    /// now, over both nodes, with a 5 s interval.
    fn start(name: &str, release_in: u64) -> Result<Self, Failed> {
        let dir = std::env::temp_dir().join("cheshire-cluster-tests");
        fs::create_dir_all(&dir).map_err( |e| format!( "create {}: {e}", dir.display() ) )?;
        let log        = dir.join( format!("{name}.log") );
        let release_at = Timestamp::now() + Duration::from_secs(release_in);
        let child      = Command::new(BIN)
            .args(["--release-at", &release_at.to_string(), "-i", "5", NODES])
            .env("RUST_LOG", "info")
            .stdout( Stdio::null() )
            .stderr(File::create(&log).map_err( |e| format!( "create {}: {e}", log.display() ) )?)
            .spawn()
            .map_err( |e| format!("start {BIN}: {e}") )?;
        Ok( Self { child, log } )
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Fails unless the daemon's log contains `text`.
    fn has(&self, text: &str) -> Result<(), Failed> {
        if self.log().contains(text) {
            Ok( () )
        } else {
            Err( format!( "log {} lacks {text:?}", self.log.display() ).into() )
        }
    }

    /// Like `has`, but gives the daemon up to `secs` seconds to log it.
    fn wait_log(&self, text: &str, secs: u64) -> Result<(), Failed> {
        wait_for( secs, || self.log().contains(text) );
        self.has(text)
    }

    fn signal(&self, sig: libc::c_int) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, sig) };
    }

    /// SIGKILL, as in a crash, and reaps the process.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Waits up to `secs` seconds for the daemon to exit on its own.
    fn wait_exit(&mut self, secs: u64) -> Result<ExitStatus, Failed> {
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            if let Some(status) = self.child.try_wait().map_err( |e| e.to_string() )? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err( format!( "daemon still running after {secs} s; log {}", self.log.display() ).into() );
            }
            sleep( Duration::from_millis(200) );
        }
    }
}

impl Drop for Daemon {
    /// Never leaves a daemon running behind a failed check.
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            self.kill();
        }
    }
}

// Cluster helpers

/// Resets the cluster when created and again when dropped, so a scenario
/// starts clean and leaves the cluster clean even when a check fails.
struct Clean;

impl Clean {
    fn new() -> Result<Self, Failed> {
        reset()?;
        Ok(Clean)
    }
}

impl Drop for Clean {
    fn drop(&mut self) {
        let _ = reset();
    }
}

/// Kills any stray daemon, cancels all jobs, resumes both nodes, and waits
/// until they show plain "idle".
fn reset() -> Result<(), Failed> {
    sh( "pkill", &["-KILL", "-f", &format!("^{BIN}")] );
    sh( "scancel", &["-u", "root"] );
    admin(&["nodename=c[1-2]", "state=resume"]);
    check( "cluster reset: both nodes idle", wait_for( 20, || state("c1") == "idle" && state("c2") == "idle" ) )
}

/// Runs a command and returns its trimmed stdout; empty if it fails to start.
fn sh(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .stderr( Stdio::null() )
        .output()
        .map( |o| String::from_utf8_lossy(&o.stdout).trim().to_string() )
        .unwrap_or_default()
}

/// An admin's `scontrol update`.
fn admin(args: &[&str]) {
    let mut all = vec!["update"];
    all.extend_from_slice(args);
    sh("scontrol", &all);
}

/// Submits a batch job to the normal partition and returns its id.
fn sbatch(args: &[&str]) -> Result<String, Failed> {
    let mut all = vec!["--parsable", "-p", "normal", "-D", "/data"];
    all.extend_from_slice(args);
    let id = sh("sbatch", &all);
    if id.is_empty() {
        Err( format!("sbatch {args:?} failed").into() )
    } else {
        Ok(id)
    }
}

/// Node state as sinfo shows it, e.g. "idle", "draining" or "drained".
/// UNDRAIN and RESUME set NO_RESPOND until slurmd re-registers, a second or so
/// later (node_mgr.c:1559), which sinfo shows as a trailing "*", e.g. "idle*".
/// sinfo prints one line per partition, hence the first line only.
fn state(node: &str) -> String {
    first_line( sh("sinfo", &["-h", "-N", "-n", node, "-o", "%T"]) )
}

/// Node's drain or down reason text ("none" if it has none).
fn reason(node: &str) -> String {
    first_line( sh("sinfo", &["-h", "-N", "-n", node, "-o", "%E"]) )
}

/// Node's ResumeAfterTime, i.e. our lease: "None" if no automatic resume is
/// armed, else a UTC timestamp, which compares correctly as a string.
fn resume_after(node: &str) -> String {
    sh("scontrol", &["show", "node", node])
        .split_whitespace()
        .find_map( |f| f.strip_prefix("ResumeAfterTime=") )
        .unwrap_or_default()
        .to_string()
}

/// Job state, e.g. "PENDING" or "RUNNING"; empty once the job has left the queue.
fn job_state(id: &str) -> String {
    sh("squeue", &["-h", "-j", id, "-o", "%T"])
}

fn first_line(input_text: String) -> String {
    input_text.lines().next().unwrap_or_default().to_string()
}

fn pause(secs: u64) {
    sleep( Duration::from_secs(secs) );
}

/// Polls `cond` once a second for up to `secs` seconds; true once it holds.
fn wait_for(secs: u64, cond: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        pause(1);
    }
}

// Checks: each fails the scenario with a description of what went wrong.

fn check(what: &str, ok: bool) -> Result<(), Failed> {
    if ok { Ok( () ) } else { Err( format!("{what}: failed").into() ) }
}

fn is(what: &str, got: String, want: &str) -> Result<(), Failed> {
    if got == want {
        Ok( () )
    } else {
        Err( format!("{what}: got {got:?}, want {want:?}").into() )
    }
}

fn starts(what: &str, got: String, prefix: &str) -> Result<(), Failed> {
    if got.starts_with(prefix) {
        Ok( () )
    } else {
        Err( format!("{what}: got {got:?}, want prefix {prefix:?}").into() )
    }
}
