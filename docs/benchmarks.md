# Benchmarks

Produced by `scripts/bench.ps1` (or `scripts/bench.sh`), which runs `dev.raftkv.server.Bench`. The program writes `bench-results.md`; run it on your own machine and compare. All numbers below come from one run in a 2-CPU Linux container. **Read them as relative comparisons, not as what RaftKV does in production**: all three servers share one machine, one disk and two CPUs, and the "network" is loopback.

Only numbers these benchmarks actually produced belong on a resume.

- Machine: 2 CPUs, Linux amd64, Java 21.0.12.1+1-1-24.04.4-Ubuntu, max heap 2048 MB
- Setup: 3 server nodes in one JVM, real TCP over loopback, real files with fsync; 32 concurrent closed-loop clients; 1 s warm-up, 5 s measured per row.

## 1. Writes: pipelining

| AppendEntries in flight per follower | ops/s | p50 ms | p99 ms |
|---|---|---|---|
| 1 (stop-and-wait) | 16,107 | 1.7 | 6.3 |
| 8 | 19,813 | 1.5 | 4.2 |

## 2. Reads: log vs ReadIndex vs lease

| read mode | ops/s | p50 ms | p99 ms |
|---|---|---|---|
| LOG | 21,688 | 1.4 | 3.4 |
| READ_INDEX | 59,671 | 0.5 | 1.0 |
| LEASE | 96,352 | 0.3 | 0.6 |

## 3. Scale-out over data groups (one machine: shows per-group overhead, not real scaling)

| data groups | ops/s (writes) | p50 ms | p99 ms |
|---|---|---|---|
| 1 | 21,165 | 1.4 | 2.8 |
| 2 | 18,067 | 1.6 | 3.8 |
| 4 | 16,113 | 1.8 | 4.0 |

## 4. Failover: leader stopped -> first successful write (10 trials)

| pre-vote | min ms | median ms | max ms |
|---|---|---|---|
| on | 308 | 330 | 1436 |
| off | 326 | 366 | 827 |

## 5. Catch-up of a follower that missed 3,000 writes

| snapshots | catch-up ms |
|---|---|
| off | 76 |
| every 500 entries (follower gets a snapshot) | 112 |

## What the numbers say

**Reads (2).** ReadIndex serves about 2.8× the throughput of log reads at a third of the latency: no log entry, no fsync, one heartbeat round shared by every read waiting at the time. Leases avoid even that round trip: about 4.4× log reads. Leases rely on clocks drifting less than 10% (see docs/learning/11-reads-prevote-pipelining.md).

**Pipelining and group commit (1).** The first run of this benchmark showed pipelining *slower* than stop-and-wait (about 10,000 vs 17,000 writes/s). The reason: each follower called fsync once per AppendEntries, and pipelining sends more, smaller messages. The fix is **follower-side group commit**: a follower appends, schedules one flush, and sends the success replies of every message that arrived meanwhile after that single fsync. After the fix, every write path roughly doubled (log reads above went from about 11,000 to 21,700/s) and pipelining wins: 19,800 vs 16,100 writes/s, p99 4.2 ms vs 6.3 ms. In the simulator, which delays and reorders messages like a real network, pipelining also made a lagging follower catch up 2.2× faster.

**Scale-out (3).** On one 2-CPU machine more groups cost a little throughput (21,200 → 16,100 writes/s from 1 to 4 groups): every group adds its own heartbeats, fsyncs and threads on the same two cores and the same disk. Real scale-out needs groups whose leaders sit on different machines. That's not something this setup can show, so it doesn't claim to.

**Failover (4).** From stopping the leader to the first successful write: about 330 ms median. That's the election timeout (150-300 ms) plus the client noticing (its attempt timeout) and retrying. Pre-vote adds one extra round of messages before an election, a few milliseconds; the occasional 0.8-1.4 s outliers are client retries that just missed the new leader.

**Catch-up (5).** A follower 3,000 entries behind catches up within about 100 ms either way at this size; whether it gets the log or a snapshot matters more as the log grows (a snapshot is proportional to the data, the log to the history).

## Not measured (yet)

- Open-loop latency vs offered load (SPEC §10.2): these clients are closed-loop, which hides queueing delay (coordinated omission).
- Migration impact on unaffected shards (SPEC §10.7) and multi-Raft overhead per group (§10.8).
- JMH micro-benchmarks (§10.9): JMH needs an annotation processor and a separate build setup; the project keeps its build dependency-free for now.
