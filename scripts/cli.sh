#!/usr/bin/env bash
# The command-line client against the local cluster.
#   scripts/cli.sh                  interactive
#   scripts/cli.sh put color blue   one command
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
exec java -cp target/classes dev.raftkv.client.cli.RaftKVCli --servers "$(peers "${NODES:-3}" "${BASE_PORT:-7001}")" "$@"
