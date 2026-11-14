# RaftKV

A sharded, fault-tolerant distributed key-value store built on the Raft consensus algorithm, written in **core Java 21** with no frameworks.

> **Status:** Milestone 0 (project setup). The full plan is in [SPEC.md](SPEC.md).

## Progress

| # | Milestone | Status |
|---|---|---|
| 0 | Project setup: build, wrapper, docs, smoke tests | ✅ |
| 1 | Skeleton, injectable clock/scheduler, binary codec, deterministic simulation | ⏳ next |
| 2 | Leader election | |
| 3 | Log replication | |
| 4 | Persistence (MetaStore, segmented log, simulated disk) | |
| 5 | KV state machine, sessions, client library | |
| 6 | Linearizability checker + chaos suite | |
| 7 | Real TCP transport, CLI, real cluster | |
| 8 | Snapshots and log compaction | |
| 9 | Multi-Raft + ShardController | |
| 10 | Shard migration + GC | |
| 11 | ReadIndex, lease reads, pre-vote, performance | |
| 12 | Membership changes | |
| 13 | Metrics, dashboard, benchmarks, polish | |

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
├── src/main/java/dev/raftkv/    production code
├── src/test/java/dev/raftkv/    tests
├── docs/
│   ├── design.md            design decisions, trade-offs, deviations from SPEC
│   ├── workflow.md          how we work: bundles, commits, milestones
│   └── learning/            one explainer per milestone
└── tools/Unbundle.java      unpacks the code bundles for each step
```

## License

TBD.
