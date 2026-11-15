#!/usr/bin/env bash
# Runs the benchmarks (see docs/benchmarks.md) and writes bench-results.md.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "$ROOT"
./mvnw -q -DskipTests test-compile
java -Xmx2g -cp target/classes:target/test-classes dev.raftkv.server.Bench bench-results.md
