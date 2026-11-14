# RaftKV

A sharded, fault-tolerant distributed key-value store built on the Raft consensus algorithm, written in **core Java 21** with no frameworks.

[![CI](https://github.com/praneeth-srikurmadasu/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/praneeth-srikurmadasu/raftkv/actions/workflows/ci.yml)

> **Status:** Milestone 7 (real TCP cluster, CLI) complete. The full plan is in [SPEC.md](SPEC.md).

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
| 8 | Snapshots and log compaction | ⏳ next |
| 9 | Multi-Raft + ShardController | |
| 10 | Shard migration + GC | |
| 11 | ReadIndex, lease reads, pre-vote, performance | |
| 12 | Membership changes | |
| 13 | Metrics, dashboard, benchmarks, polish | |

## Quickstart: a 3-node cluster on your machine

```powershell
# Windows (PowerShell), from the repository root
.\scripts\run-cluster.ps1 -Clean     # compiles, then starts n1, n2, n3 (ports 7001-7003), one window each
.\scripts\cli.ps1                    # interactive client: put / get / delete / cas / leader / help / quit
.\scripts\kill-node.ps1 -Id 2        # kill a node instantly; the cluster keeps working with 2 of 3
.\scripts\start-node.ps1 -Id 2       # it recovers from data\n2 and catches up
.\scripts\stop-cluster.ps1
```

```bash
# Linux / macOS / Git Bash (nodes run in the background, logs in logs/)
scripts/run-cluster.sh --clean && scripts/cli.sh put city Hyderabad && scripts/cli.sh get city
scripts/kill-node.sh 2; scripts/start-node.sh 2; scripts/stop-cluster.sh
```

Or start a node by hand: `java -cp target/classes dev.raftkv.Main --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003`.

## Using the client (Java)

```java
RaftKVCli cli = new RaftKVCli(NodeConfig.parsePeers("1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"),
        Duration.ofSeconds(15));
RaftKVClient client = cli.client();
client.put("city", "Hyderabad");                                      // -> previous value
client.get("city");                                                   // -> Optional[Hyderabad]
client.casAsync(key, Optional.of(expected), newValue);                // -> CasResult(success, current)
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
│   ├── config/              NodeConfig (command-line options)
│   ├── server/              RaftHost: the Raft groups of one process, one thread each
│   ├── common/              Bytes, NodeId, GroupId, Clock, Scheduler, Hashing, EventLoopScheduler
│   ├── codec/               BinaryWriter/Reader, Frame, FrameCodec, FrameDecoder
│   ├── io/                  Disk, RealDisk; sim/SimulatedDisk (loses unsynced data on crash)
│   ├── protocol/            sealed Message (RaftMessage | ClientRequest | ClientReply), MessageCodec
│   ├── raft/                RaftNode (election + replication), LeaderState, StateMachine, RaftConfig, Bug
│   │   ├── log/             RaftLog, InMemoryLog, SegmentedFileLog, LogEntry, EntryType
│   │   └── storage/         PersistentState, MetaStore, FileMetaStore, InMemoryMetaStore
│   ├── kv/                  KVStateMachine, SessionTable (exactly-once), KVServer, KVCodec
│   ├── client/              RaftKVClient (leader discovery, retries, async); cli/RaftKVCli
│   ├── sim/                 Simulation (virtual time, seeded event order, trace hash)
│   └── transport/           Transport interface, NioTransport (real TCP); sim/SimulatedNetwork
├── src/test/java/dev/raftkv/    tests (same package layout)
├── docs/
│   ├── design.md            design decisions, trade-offs, deviations from SPEC
│   ├── workflow.md          how we work: bundles, commits, milestones
│   └── learning/            one explainer per milestone
├── scripts/                 run-cluster, start-node, kill-node, stop-cluster, cli (.ps1 and .sh)
├── tools/Unbundle.java      unpacks the code bundles for each step
└── .github/workflows/ci.yml builds and tests every push
```

## Replaying a randomized test

Every simulation test prints its seed when it fails. Re-run exactly that execution:

```powershell
.\mvnw.cmd test "-Dtest=DeterminismTest" "-Draftkv.seed=8812734"
.\mvnw.cmd test "-Dtest=ElectionChaosTest" "-Draftkv.seeds=5000"   # more chaos seeds
.\mvnw.cmd test "-Dtest=ReplicationChaosTest" "-Draftkv.seeds=10000"
.\mvnw.cmd test "-Dtest=PersistenceChaosTest" "-Draftkv.seeds=5000"
.\mvnw.cmd test "-Dtest=KVChaosTest" "-Draftkv.seeds=3000"
.\mvnw.cmd test "-Dtest=LinearizabilityChaosTest" "-Draftkv.seeds=5000"
```

## License

TBD.
