#!/usr/bin/env bash
# Kills one node instantly (kill -9: no clean shutdown).   scripts/kill-node.sh 1
set -euo pipefail
source "$(dirname "$0")/common.sh"
[[ $# -eq 1 ]] || { echo "usage: $0 <id>"; exit 2; }
if p="$(node_pid "$1")"; then
  kill -9 "$p"; rm -f "$PID_DIR/n$1.pid"; echo "killed n$1 (pid $p)"
else
  echo "n$1 is not running"
fi
