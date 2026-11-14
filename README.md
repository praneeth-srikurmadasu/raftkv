# RaftKV

A sharded, fault-tolerant distributed key-value store built on the Raft consensus algorithm, written in **core Java 21** with no frameworks.

[![CI](https://github.com/praneeth-srikurmadasu/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/praneeth-srikurmadasu/raftkv/actions/workflows/ci.yml)

> **Status:** Milestone 5 (KV store, sessions, client) complete. The full plan is in [SPEC.md](SPEC.md).

## Progress

| # | Milestone | Status |
|---|---|---|
| 0 | Project setup: build, wrapper, docs, smoke tests | ✅ |
| 1 | Skeleton, injectable clock/scheduler, binary codec, deterministic simulation | ✅ |
| 2 | Leader election | ✅ |
| 3 | Log replication | ✅ |
| 4 | Persistence (MetaStore, segmented log, simulated disk) | ✅ |
| 5 | KV state machine, sessions, client library | ✅ |
| 6 | Linearizability checker + chaos suite | ⏳ next |
| 7 | Real TCP transport, CLI, real cluster | |
| 8 | Snapshots and log compaction | |
| 9 | Multi-Raft + ShardController | |
| 10 | Shard migration + GC | |
| 11 | ReadIndex, lease reads, pre-vote, performance | |
| 12 | Membership changes | |
| 13 | Metrics, dashboard, benchmarks, polish | |

## Using the client (Java)

```java
// The real network transport arrives in Milestone 7; today the client runs in the simulator.
RaftKVClient client = cluster.newClient(ClientConfig.defaults());
client.putAsync(Bytes.ofUtf8("city"), Bytes.ofUtf8("Hyderabad"));   // -> previous value
client.getAsync(Bytes.ofUtf8("city"));                                // -> Optional[Hyderabad]
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
│   ├── common/              Bytes, NodeId, GroupId, Clock, Scheduler, Hashing, EventLoopScheduler
│   ├── codec/               BinaryWriter/Reader, Frame, FrameCodec, FrameDecoder
│   ├── io/                  Disk, RealDisk; sim/SimulatedDisk (loses unsynced data on crash)
│   ├── protocol/            sealed Message (RaftMessage | ClientRequest | ClientReply), MessageCodec
│   ├── raft/                RaftNode (election + replication), LeaderState, StateMachine, RaftConfig, Bug
│   │   ├── log/             RaftLog, InMemoryLog, SegmentedFileLog, LogEntry, EntryType
│   │   └── storage/         PersistentState, MetaStore, FileMetaStore, InMemoryMetaStore
│   ├── kv/                  KVStateMachine, SessionTable (exactly-once), KVServer, KVCodec
│   ├── client/              RaftKVClient (leader discovery, retries, async)
│   ├── sim/                 Simulation (virtual time, seeded event order, trace hash)
│   └── transport/           Transport interface; sim/SimulatedNetwork
├── src/test/java/dev/raftkv/    tests (same package layout)
├── docs/
│   ├── design.md            design decisions, trade-offs, deviations from SPEC
│   ├── workflow.md          how we work: bundles, commits, milestones
│   └── learning/            one explainer per milestone
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
```

## License

TBD.
