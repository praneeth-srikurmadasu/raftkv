#!/usr/bin/env bash
# Jepsen-lite: real processes, kill -9 and shard moves under load, then a linearizability check.
#   scripts/jepsen-lite.sh [seconds]   (default 60)
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
./mvnw -q -DskipTests test-compile
exec java -cp target/classes:target/test-classes dev.raftkv.server.JepsenLite "${1:-60}"
