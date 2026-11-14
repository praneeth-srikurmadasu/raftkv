# RaftKV design notes

This document grows with every milestone. It records **what** we built, **why** we chose it over the alternatives, and every place where we deviated from `SPEC.md`.

## 1. Guiding principles

1. **JDK only in production code.** Test libraries (JUnit 5, AssertJ, jqwik, JMH) are allowed in test scope only. `pom.xml` enforces this by giving every dependency `<scope>test</scope>`.
2. **Determinism first.** Time, randomness, scheduling, network and disk are injected, so a whole cluster can run on one thread in tests and any failure can be replayed from its seed (SPEC §3.2).
3. **The paper is the source of truth.** Every Figure 2 rule is a commented line of code, e.g. `// Fig.2 AppendEntries rule 3`.
4. **Untrusted input is hostile.** Anything read from the network or disk is validated before use; decoding failures are always `CodecException`.

## 2. Build and tooling decisions

| Decision | Choice | Why |
|---|---|---|
| Build tool | Maven 3.9.11 via the Maven Wrapper | Declarative, the most common build tool in Java shops; the wrapper pins the version so every machine and CI build the same way. |
| Java version | 21 (LTS) | Records, sealed interfaces, pattern-matching `switch`, virtual threads are all final features in 21. |
| Version checks | `maven-enforcer-plugin` | Building with JDK 17 by accident fails immediately with a clear message instead of strange compile errors. |
| Pinned plugin versions | All plugins pinned in `pom.xml` | Maven's built-in defaults for old plugins change between Maven versions; pinning makes builds reproducible. |
| Base package | `dev.raftkv` | SPEC suggests `dev.<you>.raftkv`; a short neutral package keeps imports readable. |
| CI | GitHub Actions, `./mvnw -B verify` on Ubuntu + Temurin 21 | Every push is built and tested on a clean machine. |

## 3. Architecture (so far)

```
 protocol   sealed Message + MessageCodec      frames ⇄ messages
 codec      BinaryWriter/Reader, FrameCodec    bytes  ⇄ frames
 transport  Transport ── SimulatedNetwork      (NioTransport in M7)
 sim        Simulation                         virtual time, seeded event order, trace hash
 common     Clock, Scheduler, Bytes, ids, Hashing, EventLoopScheduler
```

### Key decisions (Milestone 1)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Raft communication style | One-way messages; replies are ordinary messages | Request/response RPC with futures | Raft already tolerates loss, duplication and reordering; one-way messages keep the core free of futures and timeouts and match etcd/raft. Correlation ids are reserved for client RPCs. |
| Byte payloads | Immutable `Bytes` value class | `byte[]` in records | Arrays use identity equality, which silently breaks record `equals` and round-trip tests; `Bytes` also guarantees immutability. |
| Node ids | `record NodeId(int)` | String names | 4 bytes on the wire, cheap comparisons; friendly names can be mapped in the CLI. |
| Frame integrity | CRC32C over the whole frame including the length field | No checksum (trust TCP) | TCP's 16-bit checksum misses real corruption; CRC32C is hardware-accelerated in the JDK. |
| Untrusted lengths | Checked against bytes present before allocating; frame payload capped (16 MiB default) | Trust the prefix | Prevents out-of-memory crashes from corrupt or hostile input. |
| Event ordering in simulation | `(time, seeded tie-breaker, sequence)` | Insertion order for ties | Seeded tie-breaking explores different interleavings of simultaneous events across seeds while staying reproducible per seed. |
| Simulated messages | Encoded and decoded on every hop | Pass objects directly | Exercises the production codec constantly and guarantees nodes never share mutable state. |

### Key decisions (Milestone 2)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Persisting term/vote | Synchronous `MetaStore.save` on the event loop, before the dependent message is sent | Async fsync with completion events | Simplest correct ordering; Milestone 4 revisits this with real fsync costs. |
| Candidate retries | Re-send RequestVote every heartbeat interval to peers that haven't answered | One-shot requests | §5.1 asks for retries; one lost message would otherwise waste a whole election timeout. |
| Leader stickiness | Reject RequestVote without adopting its term when we are leader, or heard from the leader within `electionTimeoutMin` | No stickiness until pre-vote | Stops removed/rejoining servers from forcing elections and is needed later for lease reads (SPEC §3.4 d). |
| Same-term second leader | `IllegalStateException` in the node | Silently follow | Two leaders in a term means Election Safety is broken; failing loudly lets the simulation report the seed. |
| Invariant checks | Run after every simulated event via `Simulation.addInvariant` | Check only at the end of a run | A violation is reported at the exact moment it happens, with the seed and virtual time. |
| Debugging aid | `Simulation.echoTraceTo(sink)` streams every trace line | Only the last 200 lines | Needed to understand failures that start long before they are detected. |

### Key decisions (Milestone 3)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Replication trigger | Send on every proposal, on every heartbeat tick, and right after a success reply while the follower is behind | Only on heartbeat ticks | Low commit latency without a separate pipeline; true bounded pipelining comes in Milestone 11. |
| Overlapping messages | Allowed; followers are idempotent and the leader uses `matchIndex` from each reply | One outstanding AppendEntries per follower | Simpler, and correctness doesn't depend on message order (Raft tolerates reordering). |
| Flow control | A success reply triggers the next send only if it advanced `matchIndex`; a rejection triggers a retry only if it lowered `nextIndex` | Send on every reply | Sending on every reply caused unbounded message amplification on duplicating networks (found by the stress run: out-of-memory); this rule keeps at most a few chains per follower. |
| `matchIndex` in replies | Follower reports `prevLogIndex + entries.size()` | Leader infers from what it sent | Lets the leader handle duplicated and reordered replies safely. |
| Rejections | Only lower `nextIndex`, never below `matchIndex + 1` | Apply conflict hints blindly | A rejection may be stale; this bound keeps it harmless. |
| Batch size | `maxEntriesPerAppend` (default 64) | Byte-based limits | Entry count is enough until real payloads (Milestone 5); byte limits arrive with flow control in Milestone 11. |
| Applying | Synchronously on the event loop, immediately after `commitIndex` advances | Separate applier thread | The state machine is in memory; a dedicated applier is only worth it for slow state machines. |
| Overwriting a committed entry | `IllegalStateException` in the follower | Silently truncate | It can only happen if safety is already broken; failing loudly reports the seed. |
| Volatile state machine | Re-applied from index 1 after a restart | Persist `lastApplied` | Matches Figure 2 (commitIndex/lastApplied are volatile); snapshots remove the replay in Milestone 8. |

### Known limitation (until Milestone 11)
A node that misses heartbeats (briefly partitioned) can time out right after it reconnects; its higher term then makes the healthy leader step down through Fig. 2's "higher term → follower" rule. Safety is unaffected, but it costs an election. Pre-vote removes this. The chaos test lets the group settle for 2 s after healing before checking stability.

## 4. Deviations from SPEC.md

1. **Message package.** SPEC places `Message` in `codec/` and the RPC records in `raft/rpc/`. Java only allows a `sealed` interface to permit classes in *other* packages when the code is a named module (`module-info.java`); in an ordinary classpath project all permitted classes must share one package. Adding a module would complicate testing for no real benefit, so `Message`, all message records and `MessageCodec` live together in `dev.raftkv.protocol`. `codec/` keeps the generic byte- and frame-level code.
2. **Simulation location.** SPEC lists `Simulation` under the test tree but `SimulatedNetwork` under `src/main`. Since the network depends on the simulation, both live in `src/main` (`dev.raftkv.sim`, `dev.raftkv.transport.sim`). This also lets the benchmarks and demos reuse them.
3. **Correlation ids for Raft.** SPEC §5.2 describes request/response matching by correlation id. Raft messages don't use it (see "communication style" above); it remains in the frame for client RPCs.
4. **Bug switches.** SPEC §9.6 describes `static final` flags enabled through a separate build profile. Instead, bugs are an `EnumSet` inside `RaftConfig` (`RaftConfig.defaults()` has none). This lets one test run compare buggy and correct code side by side with no special build, at the cost of one set lookup per guarded line. Only test code ever enables a bug.
5. **Bug detection by targeted tests.** `COMMIT_INDEX_FROM_LAST_LOG_INDEX` is detected by a rule-level test, because this leader design never creates the situation where the bug does damage. `COMMIT_PREVIOUS_TERM_BY_COUNT` is detected by the scripted Figure 8 test, because random chaos reaches a Figure 8 shape too rarely to rely on (see docs/learning/03-log-replication.md §8).
