#!/usr/bin/env bash
# Starts (or restarts after a kill) one node; it recovers from data/nN.   scripts/start-node.sh 2
set -euo pipefail
source "$(dirname "$0")/common.sh"
[[ $# -eq 1 ]] || { echo "usage: $0 <id>"; exit 2; }
start_node "$1" "${NODES:-3}" "${BASE_PORT:-7001}"
