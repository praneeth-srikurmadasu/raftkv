#!/usr/bin/env bash
# Starts a local RaftKV cluster in the background (logs in logs/nN.log).
#   scripts/run-cluster.sh            3 nodes on ports 7001-7003
#   scripts/run-cluster.sh --clean    delete all data first
#   NODES=5 BASE_PORT=8001 scripts/run-cluster.sh
set -euo pipefail
source "$(dirname "$0")/common.sh"
NODES="${NODES:-3}"; BASE_PORT="${BASE_PORT:-7001}"
if [[ "${1:-}" == "--clean" ]]; then
  for ((i = 1; i <= NODES; i++)); do
    if node_pid "$i" >/dev/null; then echo "n$i is still running; run scripts/stop-cluster.sh first"; exit 1; fi
  done
  rm -rf "$ROOT/data" "$LOG_DIR"; echo "deleted data/ and logs/"
fi
build
for ((i = 1; i <= NODES; i++)); do start_node "$i" "$NODES" "$BASE_PORT"; done
echo
echo "Talk to it with:   scripts/cli.sh"
echo "Kill a node with:  scripts/kill-node.sh 2"
echo "Stop everything:   scripts/stop-cluster.sh"
