#!/usr/bin/env bash
# The command-line client against the local cluster.
#   scripts/cli.sh                  interactive
#   scripts/cli.sh put color blue   one command
set -euo pipefail
source "$(dirname "$0")/common.sh"
load_settings
cd "$ROOT"
exec java -cp target/classes dev.raftkv.client.cli.RaftKVCli --servers "$(peers "$TOTAL" "$BASE_PORT")" "$@"
