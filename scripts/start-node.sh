#!/usr/bin/env bash
# Starts (or restarts after a kill) one node; it recovers from data/nN.   scripts/start-node.sh 2
set -euo pipefail
source "$(dirname "$0")/common.sh"
[[ $# -eq 1 ]] || { echo "usage: $0 <id>"; exit 2; }
load_settings
if [[ "$1" -lt 1 || "$1" -gt "$TOTAL" ]]; then echo "id must be between 1 and $TOTAL"; exit 2; fi
start_node "$1"
