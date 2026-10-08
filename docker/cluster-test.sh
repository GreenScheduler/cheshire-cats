#!/usr/bin/env bash
# Runs the cluster tier of the tests (tests/cluster.rs): starts the Docker dev
# cluster, runs the tests inside the cheshire container, and tears the cluster
# down again, also when a test fails or on Ctrl-C. Exits with the test result.
#
#   docker/cluster-test.sh                     # all scenarios
#   docker/cluster-test.sh a_normal_window     # extra arguments go to the test harness
#
# KEEP_CLUSTER=1 leaves the cluster running afterward, for debugging; tear it
# down with `docker compose -f docker/compose.yml down -v`.
#
# SKIP_BUILD=1 uses the images already present instead of building them, and
# fails if any is missing. CI sets it after loading the images from its cache.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
compose=(docker compose -f "$here/compose.yml")

# Prints a message to stderr and exits with status 2, before any cluster exists.
die() {
	echo "cluster-test: $*" >&2
	exit 2
}

[ -f "$here/slurm-docker-cluster/docker-compose.yml" ] ||
	die "the slurm-docker-cluster submodule is missing; run: git submodule update --init"

# Docker must be usable before anything else: without a daemon, the container
# check below would see "no containers" and carry on to a confusing failure.
command -v docker >/dev/null ||
	die "docker is not installed (no docker command in PATH)"
if ! err=$(docker info --format '{{.ServerVersion}}' 2>&1 >/dev/null); then
	case "$err" in
		*"permission denied"*)
			die "no permission to use the Docker daemon; add yourself to the docker group and log in again ($err)" ;;
		*)
			die "cannot reach the Docker daemon; is it running? (e.g. sudo systemctl start docker) ($err)" ;;
	esac
fi
# compose.yml uses `include:`, which needs Docker Compose 2.20 or later.
compose_version=$(docker compose version --short 2>/dev/null) ||
	die "Docker Compose (the 'docker compose' plugin) is not installed"
IFS=. read -r compose_major compose_minor _ <<< "${compose_version#v}"
if [ "${compose_major:-0}" -lt 2 ] || { [ "$compose_major" -eq 2 ] && [ "${compose_minor:-0}" -lt 20 ]; }; then
	die "Docker Compose $compose_version is too old; compose.yml needs 2.20 or later for include:"
fi

# The cluster's container names are fixed, so only one can exist at a time. Refuse
# to take over (and then tear down) one that someone else started.
if docker ps -a --format '{{.Names}}' | grep -qx -E 'slurmctld|slurmdbd|mysql|c1|c2|cheshire'; then
	die "cluster containers already exist (see docker ps -a); remove them first, e.g. with: docker compose -f docker/compose.yml down -v"
fi

# Without a build, every image the cluster uses must be here already, the pulled
# ones (mariadb) included.
if [ "${SKIP_BUILD:-0}" = 1 ]; then
	for image in $("${compose[@]}" config --images | sort -u); do
		docker image inspect "$image" >/dev/null 2>&1 ||
			die "SKIP_BUILD=1, but image $image is missing"
	done
fi

# Tears the cluster down, containers and state, unless KEEP_CLUSTER=1.
teardown() {
	if [ "${KEEP_CLUSTER:-0}" = 1 ]; then
		echo "cluster-test: KEEP_CLUSTER=1, leaving the cluster running"
	else
		echo "cluster-test: tearing the cluster down"
		"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
	fi
}
trap teardown EXIT
trap 'exit 130' INT TERM

# The build caches survive teardown (see compose.yml).
docker volume create cheshire-cats-target >/dev/null
docker volume create cheshire-cats-cargo-registry >/dev/null

if [ "${SKIP_BUILD:-0}" = 1 ]; then
	echo "cluster-test: SKIP_BUILD=1, using the existing images"
else
	echo "cluster-test: building images"
	# cheshire's image is built FROM the cluster image, so build that first.
	"${compose[@]}" build --quiet slurmdbd
	"${compose[@]}" build --quiet cheshire
fi

echo "cluster-test: starting the cluster"
"${compose[@]}" up -d --quiet-pull

# Waits until slurmctld answers and shows both nodes idle.
nodes_idle() {
	[ "$(docker exec slurmctld sinfo -h -N -o '%N %T' 2>/dev/null | sort -u | tr '\n' ' ')" = "c1 idle c2 idle " ]
}
for _ in $(seq 120); do
	nodes_idle && break
	sleep 1
done
nodes_idle || die "the cluster did not come up: c1 and c2 are not idle after 120 s"
echo "cluster-test: cluster is up"

# Having just started the cluster, a missing one is a failure, not "ignored".
# Run in the background and wait: a trapped signal interrupts `wait` at once,
# whereas with a foreground child bash would act on SIGTERM (e.g. a canceled CI
# job) only after the whole test run had finished. The teardown then removes
# the container, which ends the docker exec as well.
rc=0
docker exec -e CHESHIRE_REQUIRE_CLUSTER=1 cheshire \
	cargo test --features cluster-tests --test cluster -- "$@" &
wait "$!" || rc=$?
exit "$rc"
