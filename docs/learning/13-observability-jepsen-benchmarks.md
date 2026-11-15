# Milestone 13: Seeing it, attacking it, measuring it

The last milestone adds what turns a working system into one you can operate and trust: live metrics and a dashboard, a `status` command, a Jepsen-style test against real processes, benchmarks, coverage, and a nightly CI job that runs everything ten times harder.

```
src/main/java/dev/raftkv/server/
├── MetricsServer.java     /metrics (Prometheus text), /status.json, / (dashboard), JDK HttpServer
├── Dashboard.java         the one static page: groups × nodes, and the shard map
└── Admin.java             status / membership commands over the client protocol
src/test/java/dev/raftkv/server/
├── JepsenLite.java        real processes + kill -9 + shard moves + recorded history → checker
├── JepsenLiteTest.java    a 15-second run in every build
├── ProcessCluster.java    starts and kills real server processes
└── Bench.java             the benchmarks behind docs/benchmarks.md
scripts/bench.ps1|.sh, scripts/jepsen-lite.ps1|.sh
.github/workflows/nightly.yml
pom.xml                    + "coverage" profile (JaCoCo)
```

---

## 1. Metrics and the dashboard

Every server serves HTTP on its cluster port + 1000 (n1 on 7001 → 8001; `--metrics-port` changes it, 0 turns it off):

| URL | What |
|---|---|
| `http://127.0.0.1:8001/` | live dashboard: every group × every node (role, term, commit, membership), and the shard map, refreshed every second |
| `/metrics` | plain text, one `raftkv_name{node="1",group="2"} value` per line: what Prometheus scrapes |
| `/status.json` | the same numbers as JSON (the dashboard polls this on every node) |

Published per group: role, term, leader, commit/applied/last-log/snapshot index, voters and learners, elections and pre-votes started, fsyncs, snapshots taken/installed, pending proposals and reads, whether the leader holds a lease, the configuration number, and shard counts by status. Per node: frames sent, received and dropped.

All numbers are read **on each group's own event loop** (the HTTP thread asks, the loop answers), so the Raft code stays single-threaded. The JDK's `com.sun.net.httpserver` means no dependency.

Try it: `.\scripts\run-cluster.ps1 -Clean`, open http://127.0.0.1:8001/, kill a node, move a shard (`admin move 12 2` in the CLI) and watch the shard map.

## 2. `status` in the CLI

```
raftkv> status
g0 n1 LEADER term=1 leader=n1 commit=141 applied=141 log=0..141 voters [n1, n2, n3] config=1
g1 n2 FOLLOWER term=1 leader=n1 commit=4 applied=4 log=0..4 voters [n1, n2, n3, n4] config=1 serving=22 pulling=0 frozen=0
...
```
It asks every server for its own view of every group, so you see disagreements (a node behind, a stale leader) directly.

## 3. Jepsen-lite (SPEC §9.7)

Jepsen is Kyle Kingsbury's famous tool for breaking distributed databases. This is a small version of the same idea:
1. start 3 real server processes;
2. 4 client threads run random PUT / GET / CAS on 3 keys with unique values, recording each operation's start time, end time and answer (or "unknown" on a timeout);
3. a nemesis, every 1-3 s: `kill -9` a server, start a dead one again, or move a random shard to a random group;
4. at the end, the whole history goes to the same linearizability checker the simulations use.

Unlike the simulations it isn't reproducible (real OS, real time), so it complements them: it tests the real transport, real fsync, real process crashes. A 60-second run here: 154,900 operations, 8 kill -9s, 14 shard moves, linearizable. A 15-second run is part of every build; the nightly job runs 5 minutes.

```powershell
.\scripts\jepsen-lite.ps1 -Seconds 120
```

## 4. Benchmarks

`scripts/bench.ps1` measures writes with and without pipelining, the three read modes, scale-out over groups, failover time with and without pre-vote, and follower catch-up with and without snapshots. Results and what they mean: `docs/benchmarks.md`. The most useful result: the first run showed pipelining making writes *slower*, which led to follower group commit and doubled write throughput.

## 5. CI

- **Every push** (`ci.yml`): the full build with every test, including all chaos gates at their per-build seed counts, the bug-detection suite, the real-process tests and a short Jepsen-lite.
- **Nightly** (`nightly.yml`, or by hand from the Actions tab): every chaos suite at 10,000 seeds, a JaCoCo coverage report (downloadable artifact), and a 5-minute Jepsen-lite run.

Coverage locally: `.\mvnw.cmd verify -Pcoverage`, then open `target\site\jacoco\index.html`.
