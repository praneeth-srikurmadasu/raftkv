# RaftKV

A sharded, fault-tolerant distributed key-value store built on the Raft consensus algorithm, written in **core Java 21** with no frameworks.

[![CI](https://github.com/praneeth-srikurmadasu/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/praneeth-srikurmadasu/raftkv/actions/workflows/ci.yml)

> **Status:** all 13 milestones complete: sharding with live migration, linearizable reads (ReadIndex, leases), pre-vote, membership changes, metrics dashboard, Jepsen-lite, benchmarks. The full plan is in [SPEC.md](SPEC.md).

## Progress

| # | Milestone | Status |
|---|---|---|
| 0 | Project setup: build, wrapper, docs, smoke tests | ✅ |
| 1 | Skeleton, injectable clock/scheduler, binary codec, deterministic simulation | ✅ |
| 2 | Leader election | ✅ |
| 3 | Log replication | ✅ |
| 4 | Persistence (MetaStore, segmented log, simulated disk) | ✅ |
| 5 | KV state machine, sessions, client library | ✅ |
| 6 | Linearizability checker + chaos suite | ✅ |
| 7 | Real TCP transport, CLI, real cluster | ✅ |
| 8 | Snapshots and log compaction | ✅ |
| 9 | Multi-Raft + ShardController | ✅ |
| 10 | Shard migration + GC | ✅ |
| 11 | ReadIndex, lease reads, pre-vote, performance | ✅ |
| 12 | Membership changes | ✅ |
| 13 | Metrics, dashboard, benchmarks, polish | ✅ |

## Quickstart: a sharded cluster on your machine

Three server processes; each runs a replica of the shard controller (group 0) and of 3 data groups. 64 shards are spread over the groups.

```powershell
# Windows (PowerShell), from the repository root
.\scripts\run-cluster.ps1 -Clean     # compiles, then starts n1, n2, n3 (ports 7001-7003), one window each
.\scripts\cli.ps1                    # interactive client (type 'help'); 'quit' to leave
.\scripts\cli.ps1 put city Hyderabad # or one command at a time
.\scripts\cli.ps1 config             # which group serves which shards
.\scripts\cli.ps1 status             # every server's view of every group
start http://127.0.0.1:8001/            # live dashboard (n2: 8002, n3: 8003)
.\scripts\kill-node.ps1 -Id 2        # kill a node instantly; the cluster keeps working with 2 of 3
.\scripts\start-node.ps1 -Id 2       # it recovers from data\n2 and catches up
.\scripts\stop-cluster.ps1
```

Inside the interactive client (the `raftkv>` prompt only understands these commands, not PowerShell):

```
raftkv> put city Hyderabad
raftkv> get city
raftkv> shard city                    # shard 12 -> group 1
raftkv> admin move 12 2               # live migration of that shard to group 2
raftkv> admin leave 3                 # group 3's shards move to groups 1 and 2
raftkv> admin join 3 n1,n2,n3         # and back
raftkv> admin transfer-leader 1 n2    # hand group 1's leadership to n2
raftkv> status
raftkv> quit
```

```bash
# Linux / macOS / Git Bash (nodes run in the background, logs in logs/)
scripts/run-cluster.sh --clean && scripts/cli.sh put city Hyderabad && scripts/cli.sh get city
scripts/kill-node.sh 2; scripts/start-node.sh 2; scripts/stop-cluster.sh
```

Or start a node by hand: `java -cp target/classes dev.raftkv.Main --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 [--groups 3] [--shards 64]`.

Membership changes on a real cluster: `.\scripts\run-cluster.ps1 -Clean -Spares 2` starts n1-n3 as voters and configures n4, n5 as spares; `.\scripts\start-node.ps1 -Id 4`, then `admin add-server 1 n4` in the CLI (and `admin remove-server 1 n1`).

## Benchmarks

On a 2-CPU container, 3 nodes on one machine (loopback, real fsync); details and caveats in [docs/benchmarks.md](docs/benchmarks.md):

| | ops/s | p50 ms | p99 ms |
|---|---|---|---|
| writes, pipelined (8 in flight) | 19,800 | 1.5 | 4.2 |
| writes, stop-and-wait | 16,100 | 1.7 | 6.3 |
| reads through the log | 21,700 | 1.4 | 3.4 |
| reads, ReadIndex | 59,700 | 0.5 | 1.0 |
| reads, lease | 96,400 | 0.3 | 0.6 |

Leader stopped → first successful write: 330 ms median. Run `scripts/bench.ps1` to measure your machine.

## Testing in one paragraph

Every Raft and sharding rule runs under a deterministic simulator (one seed = one exact execution) with swarm fault injection: partitions, one-way link failures, crashes that lose or tear unsynced writes, message loss, duplication, delay and reordering. After every simulated event, invariants are checked (election safety, log matching, leader completeness, state machine safety, single shard owner, no double apply), and every client history is checked for linearizability. Each build runs thousands of such runs (2,000 sharded runs with constant reconfiguration alone), 20 deliberately injected bugs must each be caught, and Jepsen-lite attacks real processes with kill -9 and shard moves. A nightly job runs every suite at 10,000 seeds.

## Using the client (Java)

```java
try (RaftKVCli cli = new RaftKVCli(NodeConfig.parsePeers("1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"),
        Duration.ofSeconds(15))) {
    KVClient client = cli.client();       // sharded: key -> shard -> group -> leader
    client.put("city", "Hyderabad");      // -> previous value
    client.get("city");                   // -> Optional[Hyderabad]
    client.cas("city", "Hyderabad", "Vizag");
}
```

## Build and test

You only need **JDK 21** and **Git**. Maven is downloaded automatically by the Maven Wrapper (`mvnw`).

```powershell
# Windows (PowerShell)
.\mvnw.cmd test
```

```bash
# Linux / macOS
./mvnw test
```

## Repository layout

```
raftkv/
├── SPEC.md                  full build specification
├── pom.xml                  Maven build (JDK-only main code; test libraries only in test scope)
├── mvnw, mvnw.cmd, .mvn/    Maven Wrapper: pins Maven 3.9.11 for everyone
├── src/main/java/dev/raftkv/
│   ├── Main.java            starts one server process
│   ├── config/              NodeConfig (command-line options), ClusterBootstrap (first configuration)
│   ├── server/              RaftHost, MessageRouter, Replicas (multi-Raft); Admin; MetricsServer + Dashboard
│   ├── shard/               controller (configs, Rebalancer), ShardKVStateMachine, ShardKVServer (migration), ShardedKVClient
│   ├── common/              Bytes, NodeId, GroupId, Clock, Scheduler, Hashing, EventLoopScheduler
│   ├── codec/               BinaryWriter/Reader, Frame, FrameCodec, FrameDecoder
│   ├── io/                  Disk, RealDisk; sim/SimulatedDisk (loses unsynced data on crash)
│   ├── protocol/            sealed Message (RaftMessage | ClientRequest | ClientReply), MessageCodec
│   ├── raft/                RaftNode (election, pre-vote, replication, reads, membership), Membership, RaftConfig, Bug
│   │   ├── log/             RaftLog, InMemoryLog, SegmentedFileLog, LogEntry, EntryType
│   │   └── storage/         MetaStore, FileMetaStore, SnapshotStore, FileSnapshotStore (+ in-memory versions)
│   ├── kv/                  KVStateMachine, SessionTable (exactly-once), KVServer, KVCodec
│   ├── client/              KVClient, SessionClient (sessions, retries, leader discovery), RaftKVClient, AdminClient; cli/RaftKVCli
│   ├── sim/                 Simulation (virtual time, seeded event order, trace hash)
│   └── transport/           Transport interface, NioTransport (real TCP); sim/SimulatedNetwork
├── src/test/java/dev/raftkv/    tests (same package layout)
├── docs/
│   ├── design.md            design decisions, trade-offs, deviations from SPEC
│   ├── benchmarks.md        what the benchmarks measured and what it means
│   ├── workflow.md          how we work: bundles, commits, milestones
│   └── learning/            one explainer per milestone
├── scripts/                 run-cluster, start-node, kill-node, stop-cluster, cli, bench, jepsen-lite (.ps1 and .sh)
├── tools/Unbundle.java      unpacks the code bundles for each step
└── .github/workflows/ci.yml builds and tests every push
```

## Replaying a randomized test

Every simulation test prints its seed when it fails. Re-run exactly that execution:

```powershell
.\mvnw.cmd test "-Dtest=DeterminismTest" "-Draftkv.seed=8812734"
.\mvnw.cmd test "-Dtest=KVChaosTest" "-Draftkv.seed=8812734" "-Draftkv.trace=true"   # print every event
.\mvnw.cmd test "-Dtest=ElectionChaosTest" "-Draftkv.seeds=5000"   # more chaos seeds
.\mvnw.cmd test "-Dtest=ReplicationChaosTest" "-Draftkv.seeds=10000"
.\mvnw.cmd test "-Dtest=PersistenceChaosTest" "-Draftkv.seeds=5000"
.\mvnw.cmd test "-Dtest=KVChaosTest" "-Draftkv.seeds=3000"
.\mvnw.cmd test "-Dtest=LinearizabilityChaosTest" "-Draftkv.seeds=5000"
.\mvnw.cmd test "-Dtest=ShardedChaosTest" "-Draftkv.seeds=5000"
.\mvnw.cmd test "-Dtest=MembershipChaosTest" "-Draftkv.seeds=2000"
```

## Bugs the tests must catch

Each bug below can be switched on in tests only (`RaftConfig.bugs`); a test enables it and requires the harness to fail. Seed of first detection from a recent run (chaos seeds are random per run, so these vary):

| Injected bug | Caught by | First seed |
|---|---|---|
| `VOTE_WITHOUT_LOG_CHECK` | Leader Completeness | 1-10 |
| `VOTE_TWICE_IN_TERM` | Election Safety | 2-4 |
| `VOTEDFOR_NOT_PERSISTED` | Election Safety after crash (tested with pre-vote off) | 7-93 |
| `LEADER_IGNORES_HIGHER_TERM` | Election Safety (tested with pre-vote off, which hides it) | 3 |
| `COMMIT_PREVIOUS_TERM_BY_COUNT` | scripted Figure 8 test | scripted |
| `COMMIT_INDEX_FROM_LAST_LOG_INDEX` | rule-level test | scripted |
| `TRUNCATE_ON_STALE_APPEND` | Log Matching | 1 |
| `NO_NOOP_ON_ELECTION` | liveness | 1 |
| `REPLY_BEFORE_FSYNC` | commit durability (simulated disk) | 1-14 |
| `SESSION_DEDUP_DISABLED` | no double apply / linearizability | 1 |
| `READ_FROM_LOCAL_STATE` | linearizability | 2-7 |
| `SESSIONS_NOT_MIGRATED` | no double apply across groups | 2 |
| `SERVE_WHILE_BEPULLING` | single owner / linearizability | 1 |
| `SKIP_CONFIG_NUMBERS` | sharding invariants | 1 |
| `GC_BEFORE_INSERT_COMMITTED` | scripted crash between fetch and insert | scripted |
| `CHECK_OWNERSHIP_ON_RECEIVE_ONLY` | linearizability during migration | 13 |
| `READINDEX_WITHOUT_HEARTBEAT` | scripted deposed-leader read + linearizability checker | 1 |
| `LEASE_WITHOUT_STICKINESS` | scripted rival election under a lease + checker | 1 |
| `RESET_TIMER_ON_REJECTED_VOTE` | scripted liveness (only one electable node) | 1 |
| `CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT` | the 2015 membership bug, replayed step by step | 2 |

## License

TBD.
