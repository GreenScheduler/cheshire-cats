#!/bin/bash
# libslurm authenticates every RPC through the local munged, so start it
# before whatever the container runs.
set -e
gosu munge /usr/sbin/munged
exec "$@"
