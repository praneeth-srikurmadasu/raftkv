#!/usr/bin/env bash
# Starts a local RaftKV cluster in the background (logs in logs/nN.log, dashboard on port 8001).
#   scripts/run-cluster.sh            3 nodes on ports 7001-7003
#   scripts/run-cluster.sh --clean    delete all data first
#   NODES=3 SPARES=2 scripts/run-cluster.sh   3 voters, plus n4 and n5 ready to join later
set -euo pipefail
source "$(dirname "$0")/common.sh"
nodes="${NODES:-3}"; spares="${SPARES:-0}"; base="${BASE_PORT:-7001}"
if [[ "${1:-}" == "--clean" ]]; then
  for ((i = 1; i <= nodes + spares + 5; i++)); do
    if node_pid "$i" >/dev/null; then echo "n$i is still running; run scripts/stop-cluster.sh first"; exit 1; fi
  done
  rm -rf "$ROOT/data" "$LOG_DIR"; echo "deleted data/ and logs/"
fi
build
save_settings "$nodes" "$spares" "$base"
for ((i = 1; i <= nodes; i++)); do start_node "$i"; done
echo
echo "Dashboard:         http://127.0.0.1:$((base + 1000))/"
echo "Talk to it with:   scripts/cli.sh"
echo "Kill a node with:  scripts/kill-node.sh 2"
if [[ "$spares" -gt 0 ]]; then
  echo "Start a spare:     scripts/start-node.sh $((nodes + 1))   (then: scripts/cli.sh admin add-server 1 n$((nodes + 1)))"
fi
echo "Stop everything:   scripts/stop-cluster.sh"
