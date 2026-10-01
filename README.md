`cheshireCATS` runs as a separate process on a SLURM cluster and masks nodes during peak carbon intensity times. Carbon intensity forecasts are obtained from the [CATS](https://github.com/GreenScheduler/cats) carbon footprint API.

The project is under initial development. The road map is:

1. [x] Establish remote procedure calls (RPC) to `slurmctld` that drain nodes.
    - nodes will potentially need to be powered down for full power savings.
2. [ ] Call the Python CATS API to receive the carbon intensity forecast.
    - could call the HTTP server implemented in CATS.
    - alternatively, could call the Python API implemented in CATS via PyO3.
    - or shell out to call the CATS CLI. Make sure CATS is not run as root.
3. [ ] Develop an optimization algorithm that selects nodes for draining and/or powering down to achieve maximal computational resource availability given a carbon footprint budget.

# SLURM interaction

We run a separate daemon that interacts with the SLURM cluster via Remote Procedure Calls (RPC) using APIs defined in the public `slurm.h` header. The daemon holds a list of nodes (currently user defined, will be eventually algorithmically determined) and drains them for a specified interval starting at a certain time. Only nodes that nobody else has taken out of service (drained, down, or failed) when the window opens are acted on. To avoid conflict with cluster administration, the daemon periodically (at intervals set by the user) polls the SLURM node list to see if someone else has acted on one of our nodes (undrained it, re-drained it, or changed the drain reason) and drops it from further consideration. We opted for a daemon rather than a `cron` job or `systemd` timer to implement this behavior.

Each drain carries a lease that the daemon renews at every poll. If the daemon stops renewing it, `slurmctld` resumes the node on its own once the lease runs out.

# CLI

The daemon is run as root or as the `SlurmUser` (`slurmctld` accepts node updates only from them):

```sh
cheshire-cats [--drain-at T] --release-at T [-i SECS] [-l SECS] [-r TEXT] [--slurm-conf PATH] NODES...
```

- `NODES`: SLURM hostlist expressions (e.g., `node[01-04]`, `gpu07`), expanded locally, preserving the input order.
- Times: `now` (default for `--drain-at`), RFC 3339 with offset, or local `YYYY-MM-DD HH:MM[:SS]` (space or `T`). A local time that does not exist or occurs twice because of a daylight saving time change is rejected; for a repeated time, the error message gives the two offsets to choose from. This time specification is temporary and the drain start time will be determined using the CATS forecast in the future.
- `-i` interval (default 30 s), `-l` lease (default 3 x interval, must be no shorter than 2 x interval), `-r` reason text (default `carbon-aware drain`).
- Stored reason: `cheshire-cats: <text> until <local release time>`.
- Nodes unknown to `slurmctld` are skipped with a warning; the daemon exits only if none of the nodes are known.
- Exit 1 if validation fails (bad arguments, or no node known to `slurmctld`), or at the end any node is still held (not released) or still waiting (never drained). Otherwise 0.

Stopping the daemon early with SIGINT (Ctrl-C) or SIGTERM releases any held nodes before exiting. If it is killed outright (SIGKILL) or crashes, the nodes stay drained until the lease runs out; a new run started before then adopts them.

## Limitations

1. Fails open: if the daemon cannot reach `slurmctld` for longer than the lease, `slurmctld` releases the nodes even though the daemon is still running.
2. The daemon polls and then updates the nodes in separate RPC calls, so an admin's drain reason change made between the two may be overridden by us.

# Building

Clone the repository:

```sh
git clone https://github.com/GreenScheduler/cheshire-cats
```

Building requires `libslurm` and the SLURM headers, including `slurm_version.h` (generated when SLURM is built; distributions ship it in the development package, e.g. `slurm-devel` or `libslurm-dev`). By default, we look for them under `/usr` (`/usr/include/slurm` and `/usr/lib64` or `/usr/lib`). In that case, simply run

```sh
cargo build --release
```

in the cheshire-cats directory. You can optionally specify a custom SLURM installation prefix by setting the `SLURM_PREFIX` variable, for example:

```sh
SLURM_PREFIX=/opt/slurm cargo build --release
```

# Testing

Running tests is optional for users. Unit tests do not require a functioning cluster and can be run with

```sh
cargo test
```

Integration tests run on a Docker container cluster, implemented in the GreenScheduler fork of [`giovtorres/slurm-docker-cluster`](https://github.com/giovtorres/slurm-docker-cluster). It is included as the `docker/slurm-docker-cluster` submodule, so clone the repository with

```sh
git clone --recurse-submodules https://github.com/GreenScheduler/cheshire-cats
```

or run `git submodule update --init` in an existing clone. The submodule is not required for regular user builds.

The machine needs a running Docker daemon, Docker Compose 2.20 or later, and permission to use Docker (e.g., membership in the `docker` group). The test runner builds and starts the Docker cluster, runs the tests inside it, and tears the cluster down, also when a test fails or the run is interrupted. Run it from the cheshire-cats directory with

```sh
docker/cluster-test.sh
```

Important considerations:

- the cluster tests take time: about 3 min for the tests themselves, plus building the images and starting the cluster. The first run builds SLURM from source and takes considerably longer.
- the runner refuses to start if containers from a previous dev cluster still exist.
- **never** manually set `CHESHIRE_DEV_CLUSTER=1` on a production cluster, because the tests cancel all root jobs, as well as draining and resuming nodes. We do check other parameters of the cluster before running the tests, but these may change in the future, so it is safer never to set this variable. It is set automatically inside the dev cluster.
