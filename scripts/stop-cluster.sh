#!/usr/bin/env bash
# Stops every node the scripts started.
set -euo pipefail
source "$(dirname "$0")/common.sh"
shopt -s nullglob
for f in "$PID_DIR"/n*.pid; do
  id="$(basename "$f" .pid)"; id="${id#n}"
  if p="$(node_pid "$id")"; then kill "$p"; echo "stopped n$id (pid $p)"; fi
  rm -f "$f"
done
