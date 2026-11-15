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
 Main / client.cli   process entry points (server, command-line client)
 config     NodeConfig, ClusterBootstrap       options; first configuration of a new cluster
 server     RaftHost, MessageRouter, Replicas  many groups per process; routes by group id to each group's thread
 shard      controller, ShardKVStateMachine,   configs + rebalancing (group 0); shard statuses, migration, GC
            ShardKVServer, ShardedKVClient     (data groups); key → shard → group routing (clients)
 client     SessionClient, RaftKVClient        sessions, retries, leader discovery
 kv         KVServer, KVStateMachine, sessions client requests ⇄ Raft proposals (single-group mode)
 raft       RaftNode, log/, storage/           the consensus core (single-threaded), snapshots
 protocol   sealed Message + MessageCodec      frames ⇄ messages
 codec      BinaryWriter/Reader, FrameCodec    bytes  ⇄ frames
 transport  Transport ── SimulatedNetwork | NioTransport
 io         Disk ── SimulatedDisk | RealDisk
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

### Key decisions (Milestone 4)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| File access | All storage goes through a small `Disk` interface | Using `FileChannel` directly | Lets tests run on `SimulatedDisk`, which models lost, torn and reordered unsynced writes and undone renames. |
| Meta file | tmp file → fsync → atomic rename → directory fsync, CRC, fatal on corruption | In-place overwrite; append-only meta log | The rename protocol gives "old or new, never a mix" with a 25-byte file. |
| Log layout | Segment files named by first index; `[len][crc][index][term][type][data]` records | One big file | Snapshots delete whole old segments; recovery reads only what is needed. |
| Recovery | Bad record in the last segment = torn tail (truncate); in an earlier segment, or a gap = corruption (refuse to start) | Always truncate | Guessing could silently drop acknowledged entries. Valid because the rolling and truncation rules confine crash damage to the last segment. |
| Rolling | fsync old segment + directory before creating the next | fsync only on `sync()` | Without it a crash could keep a later segment's name and lose an earlier one's (found by the property test). |
| Truncation | Delete newest-first with a directory sync per delete, then truncate + fsync, before any new write | Batch delete + one sync | File systems persist unsynced deletes in any order; batching left gaps after crashes (found by the property test). |
| Follower durability | fsync if the log changed **or** the reply covers non-durable entries | fsync only on change | Covers entries appended during a brief leadership whose flush never ran. |
| Leader durability | Append, replicate immediately, fsync in a scheduled flush; count own copy only after it | fsync before replicating | Overlaps the leader's fsync with the network round trip (thesis §10.2.1) and batches fsyncs (group commit). |
| Windows | Directory fsync is a no-op | Fail | Windows can't open directories as channels; NTFS journals metadata. |

### Key decisions (Milestone 5)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Message hierarchy | `Message` = `RaftMessage` (has a term) ∪ `ClientRequest` ∪ `ClientReply` | Give client messages a fake term | Raft's "higher term → step down" rule must never fire on client traffic; the type system now guarantees it. |
| Exactly-once | Per-client cache of results by sequence + client-sent `ackedBelow` watermark (thesis §6.3, RIFL) | Last sequence per client | Supports many in-flight requests per client (async API); memory bounded by what's unacknowledged. |
| Client identity | Random 128-bit UUID, no registration entry | Register sessions through the log | One less round trip; a lost session is detected by `ackedBelow > 1` on an unknown client. |
| Session expiry | Leader stamps wall-clock time into each entry; state machine uses `max` of seen timestamps | Local clocks | Every replica must expire the same sessions at the same index, or replicas diverge. |
| Reads | `GET` goes through the log | ReadIndex / leases | Simple and linearizable; faster read paths are Milestone 11. |
| Bad input in apply | Becomes `INVALID_REQUEST` | Throw | A throw would crash every replica at the same entry, forever. |
| Client retries | Same sequence number; exponential backoff with full jitter; overall deadline | Fixed retry delay | Jitter avoids synchronized retry storms after a leader change. |
| Client threading | All state on one event loop; async API; blocking helpers for non-loop callers | Locks | Same model as the server; deterministic in simulation. |

### Key decisions (Milestone 6)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Checker algorithm | Wing & Gong search with Lowe's memoization (the Porcupine algorithm), per key | Brute-force permutations; Knossos-style WGL without memo | Linearizability is local, so per-key checking is exact; memoization keeps searches in milliseconds. |
| Unknown outcomes | Return time = ∞, any answer accepted; unknown GETs dropped | Treat timeouts as failures | The only correct choice: a timed-out write may or may not have happened. |
| Bounding unknowns | A client that times out is retired and replaced (max 3 per run) | Let clients keep retrying | Each unknown op stays open forever in the search; Jepsen does the same. |
| Verdicts | linearizable / proven violation / inconclusive (budget exceeded = failure) | Treat budget exhaustion as pass or as bug found | Never claim a result the search didn't prove. |
| Workload | 35% PUT, 35% GET, 20% CAS, 10% DELETE on 2-4 keys, unique values | Many keys | Few keys = many conflicts = interesting histories; unique values make stale reads unambiguous. |
| Faults | Swarm: each fault type on/off per seed, incl. one-way link failures | One fixed mix | Swarm testing finds bugs that a fixed blend of faults dilutes. |

### Key decisions (Milestone 7)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| I/O model | One NIO selector thread; frames encoded on the caller's thread | Thread per connection; Netty | JDK only; scales to many groups and clients per process. |
| Connection identity | 9-byte handshake `RKV1`, version, NodeId; connections keyed by remote id, whoever dialed | Separate inbound/outbound links per peer | Replies to clients reuse the client's connection; servers never need client addresses. |
| Duplicate connections | Newest established connection becomes the route; older ones keep receiving until closed | Tie-break by id and close one | Simple and race-free; both connections are valid TCP streams. |
| Failure handling | Best effort: broken connection → drop its queue, redial with exponential backoff + full jitter (50 ms base, 2 s cap) | Retransmit in the transport | Raft already retransmits; the transport promises nothing more (same contract as SimulatedNetwork). |
| Memory bounds | ≤ 1,024 frames waiting for a dial; ≤ 64 MiB queued per connection; then drop | Unbounded queues | A dead or slow peer must not exhaust memory. |
| Threads | Selector thread → `RaftHost` → one `EventLoopScheduler` per group | Shared pool | Keeps the Raft core single-threaded and lock-free, as in the simulator. |
| Fatal errors | Event-loop error handler halts the process (fail-stop) | Log and continue | After a failed fsync the on-disk state is unknown; restart and recover instead. |
| Client node ids | Random id ≥ 2²⁴ announced in the handshake | Separate client protocol | Same framing and code path as peers; ids can't collide with server ids in practice. |

### Key decisions (Milestone 8)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Trigger | `snapshotThreshold` applied entries not yet covered (default 10,000; 0 = off) | Log size in bytes | Simple, deterministic in simulation; bytes can be added with metrics. |
| Order of operations | serialize → save durably → only then `compactPrefix` | Compact first | A crash in between must never lose committed entries. |
| Storage | One file per group, tmp → fsync → rename → fsync dir; CRC32C; corruption is fatal | Keep N old snapshots | The log prefix is gone once compacted, so a bad snapshot can't be repaired from a local fallback anyway; the next InstallSnapshot from a leader can. |
| Recovery | Load snapshot, keep log entries after it if `(index, term)` matches, otherwise reset the log | Always reset | Keeps the tail a crash left behind. |
| InstallSnapshot | Chunked by offset; next chunk only after the previous is acknowledged; follower accepts strictly in order | Pipeline chunks | Same "send more only on progress" rule that fixed message amplification in M3. |
| Testing | `SnapshotSwarm`: every chaos suite snapshots at random tiny thresholds in 2/3 of seeds | Separate snapshot chaos test | Compaction then meets every fault the other suites inject. |

### Key decisions (Milestone 9)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Partitioning | 64 fixed shards, FNV-1a 64 mod N | Consistent hashing; ranges | Explicit, tiny routing table; keys never change shard. Ranges are a stretch goal. |
| Rebalancer | Pure function; the `N mod G` extra shards go to the groups holding the most (ties: lowest gid) | SPEC: extra shards to the lowest gids | Giving extras to the current largest holders is what makes the move count minimal; SPEC's rule can force needless moves. |
| Controller ops | Join/Leave/Move/Query through group 0's log with sessions | Direct state reads | Exactly-once admin commands; Query is linearizable. |
| Hosting | Every process hosts the controller and every data group (`--groups`) | Separate processes per group | Simplest real deployment; the simulator also runs separate layouts (group g on its own three nodes). |
| Routing | `MessageRouter` by group id; same code in simulator and processes | Separate routing for real network | Chaos tests cover the production routing. |
| Bootstrap | Lowest-id node joins all data groups once, through a session | Static config file | Exactly one initial configuration; later changes only via admin commands. |
| Client | Cached config, cached leader per group; same sequence number on every retry, to any group | New sequence after WRONG_GROUP | Required for exactly-once across migrations (sessions move with shards). |

### Key decisions (Milestone 10)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Migration state | Shard statuses in the replicated state machine; poller and migrator derive work from them | Leader-memory work queues | A leader crash loses nothing; a new leader resumes. |
| Config steps | Exactly `current + 1`, only when all shards are settled | Jump to latest | A shard that moved twice could be pulled from the wrong group. |
| Data movement | Pull: new owner fetches from any replica of the old owner that applied the config | Push from old owner | Receiver drives retries; frozen (BEPULLING) data is identical on every replica. |
| Idempotence | Every ShardOp carries its config number and is ignored unless it matches | Message dedup ids | Duplicates, retries and reordering are harmless by construction. |
| GC | DeleteShard only after InsertShards is committed at the new owner; old owner deletes through its log; GcDone completes | Delete on fetch | Deleting earlier can lose the shard if the new leader crashes before the insert commits. |
| Sessions | Per shard, migrated with the data | Global per group | Exactly-once survives a key moving between groups mid-retry. |
| Ownership check | On arrival (cheap rejection) and again at apply (decisive) | Arrival only | A ConfigChange can land between arrival and commit. |

### Key decisions (Milestone 11)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Read modes | LOG, READ_INDEX (default), LEASE, per group (`RaftConfig.readMode`) | One fixed mode | SPEC asks to implement and benchmark all three; LEASE adds a clock assumption, so it is opt-in. |
| Leadership confirmation | A heartbeat round number in every AppendEntries, echoed in replies | Correlation ids per read; a separate RPC | One field; any reply of the current term counts; concurrent reads share a round. |
| Lease length | `electionTimeoutMin / (1 + maxClockDrift)` from the confirmed round's send time; dropped during leadership transfer | From the reply time | Send time is the conservative end; transfer bypasses stickiness. |
| Pre-vote | On by default; a pre-vote request changes nothing on the voter | Off | Stops cut-off nodes from inflating terms and deposing healthy leaders. |
| Check-quorum | On by default; checked every minimum election timeout | Off | Deposed leaders step down by themselves; makes leases safe in practice. |
| Pipelining | Window of 8 batches per follower; progress-only sends; go-back-N after a silent heartbeat; one go-back per gap | Unbounded; optimistic nextIndex | Bounded, cannot amplify messages (each send is caused by progress or a timer). |
| Follower group commit | Success replies wait for the next flush; one fsync covers every AppendEntries before it | fsync per message | Doubled write throughput; made pipelining pay off (docs/benchmarks.md). |
| Read bugs | Scripted windows judged by the linearizability checker | Chaos only | Check-quorum and stickiness close most of the window; random faults rarely hit it. |

### Key decisions (Milestone 12)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Change protocol | Single-server changes; a configuration takes effect when appended, reverts if truncated | Joint consensus | Simpler; the thesis' recommended approach. |
| Safety fix | A change waits (not refused) until an entry of the leader's term and the previous change have committed | Refuse | Callers needn't retry; the rule still holds. |
| New servers | Learner first, promoted once caught up to the commit index | Voter immediately | A voter with an empty log would stall commits. |
| Removed servers | Excluded from majorities at once, but kept receiving entries until the removal commits | Dropped at once | They learn they are out and stop campaigning. |
| Snapshots | Carry the membership at their index (prefix in the snapshot bytes) | Separate metadata file | No format change in stores or InstallSnapshot. |
| Admin path | Admin commands ride on ClientRequest with a reserved tag, answered by the replica | New message types | No protocol change; works through every router and transport. |
| Spares | `--voters` sets initial membership; other peers host every group as learners | Dynamic process join | Peers' addresses must be known to the transport anyway. |

### Key decisions (Milestone 13)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Metrics | JDK HttpServer: Prometheus text, JSON, dashboard page; values read on each group's loop | JMX, Micrometer | No dependencies; scrapeable; thread-safe by construction. |
| Dashboard | One static page polling every node's /status.json (CORS) | Server-side aggregation | Shows each node's own view; works with nodes down. |
| Jepsen-lite | Java program + short JUnit run; same checker as the simulations | Real Jepsen (Clojure) | Same language, same checker, runs in CI. |
| Coverage | JaCoCo in an opt-in Maven profile, nightly | Always on | Keeps the normal build fast and dependency-free. |

## 4. Deviations from SPEC.md

1. **Message package.** SPEC places `Message` in `codec/` and the RPC records in `raft/rpc/`. Java only allows a `sealed` interface to permit classes in *other* packages when the code is a named module (`module-info.java`); in an ordinary classpath project all permitted classes must share one package. Adding a module would complicate testing for no real benefit, so `Message`, all message records and `MessageCodec` live together in `dev.raftkv.protocol`. `codec/` keeps the generic byte- and frame-level code.
2. **Simulation location.** SPEC lists `Simulation` under the test tree but `SimulatedNetwork` under `src/main`. Since the network depends on the simulation, both live in `src/main` (`dev.raftkv.sim`, `dev.raftkv.transport.sim`). This also lets the benchmarks and demos reuse them.
3. **Correlation ids for Raft.** SPEC §5.2 describes request/response matching by correlation id. Raft messages don't use it (see "communication style" above); it remains in the frame for client RPCs.
4. **Bug switches.** SPEC §9.6 describes `static final` flags enabled through a separate build profile. Instead, bugs are an `EnumSet` inside `RaftConfig` (`RaftConfig.defaults()` has none). This lets one test run compare buggy and correct code side by side with no special build, at the cost of one set lookup per guarded line. Only test code ever enables a bug.
5. **Bug detection by targeted tests.** `COMMIT_INDEX_FROM_LAST_LOG_INDEX` is detected by a rule-level test, because this leader design never creates the situation where the bug does damage. `COMMIT_PREVIOUS_TERM_BY_COUNT` is detected by the scripted Figure 8 test, because random chaos reaches a Figure 8 shape too rarely to rely on (see docs/learning/03-log-replication.md §8).
6. **Durable state machine.** Until Milestone 8 the state machine was rebuilt by replaying the whole log after a restart. Since Milestone 8 it is restored from the latest snapshot plus the log after it.
7. **No session registration.** SPEC §4.2 allows registering sessions through the log; clients instead pick a random 128-bit id and the first request (with `ackedBelow == 1`) creates the session. Losing a session is still detected (`ackedBelow > 1` on an unknown client → `SESSION_EXPIRED`).
8. **RaftLog is not AutoCloseable.** It has a plain `close()` so IDEs don't flag every in-memory or node-owned log as a resource leak.
9. **One listening port.** SPEC §5.2 suggests separate ports for peer and client RPCs. One port serves both; the handshake's node id tells them apart. A second port can be added when there's a reason (e.g. firewalling clients from peers).
10. **No correlation-id request futures in the transport.** SPEC §5.2 describes request/response matching by correlation id inside the transport. The transport is one-way messaging; `RaftKVClient` already matches replies by sequence number and has its own timeouts, so frames carry correlation id 0.
11. **Back-pressure drops instead of replying `OVERLOADED`.** Per-connection write queues are bounded (64 MiB), but a full queue drops frames rather than surfacing `OVERLOADED` to clients. Admission control comes with the performance work (Milestone 11).
12. **fsync on the group's event loop.** SPEC §3.2 prefers slow work off the event loop. The group-commit flush (leader and, since Milestone 11, follower) runs on the group's own thread, batching every append made before it. A dedicated I/O thread per group would let the loop keep working during the fsync; not done yet.
13. **Coalesced heartbeats** (SPEC §5.2 stretch goal) are deferred. With 4 groups per process the extra heartbeats are negligible; they matter at hundreds of groups.
14. **Snapshots serialize on the event loop.** SPEC §3.8 asks for a copy-on-write capture and background serialization. The group pauses while its state is copied (milliseconds for megabytes). Not done yet.
15. **Incoming snapshot chunks are buffered in memory**, then saved with the same crash-safe write as local snapshots, instead of being appended to a temp file chunk by chunk. Memory is bounded by one snapshot.
16. **Controller Query goes through the log.** SPEC §6.3 says ReadIndex. ReadIndex exists since Milestone 11 and serves KV GETs, but the controller's Query still goes through the log: correct, slower, and it makes the controller's log grow with every config poll (compacted by snapshots).
17. **Rebalancer tie rule** differs from SPEC §6.3: the extra shards go to the groups that currently hold the most (lowest gid on ties) rather than always to the lowest gids, because that is what guarantees the minimum number of moves (see Milestone 9 decisions).
18. **Real processes host every data group.** `Main` runs a replica of the controller and of each of `--groups` data groups on every process; `admin join` can only (re)add groups that the processes host. Groups on separate machine sets are exercised in the simulator. Placing groups on subsets of processes (and SPEC §9.7's 9-JVM Jepsen-lite) is part of Milestone 13.
19. **Sharded gate is 2,000 seeds per build.** Matches SPEC §11; larger runs are done by hand with `-Draftkv.seeds` (2,500 seeds: 1.4M operations, 20,233 reconfigurations, all linearizable).
20. **Read-bug detection is scripted.** SPEC §9.6 expects chaos to catch `READINDEX_WITHOUT_HEARTBEAT` and `LEASE_WITHOUT_STICKINESS`. Check-quorum and stickiness shrink the window in which they do harm so much that 300 chaos seeds caught neither; scripted scenarios drive the system into that window and the linearizability checker judges (ReadBugDetectionTest). `RESET_TIMER_ON_REJECTED_VOTE` is caught by a scripted liveness test.
21. **Membership chaos runs on the single-group store.** SPEC §11 asks for 3→5→3 in a data group while shards migrate. The churn chaos test (`MembershipChaosTest`) uses the single-group KV store; the sharded path uses the same RaftNode code per group, and the CLI changes any group's membership, but no chaos test combines membership churn with migration yet.
22. **Controller configurations list each group's original servers.** After `admin add-server`, a group's new member isn't in the controller's server list; clients reach it by following NOT_LEADER hints (it must be in their `--servers`).
23. **Benchmarks are partial.** Implemented: pipelining on/off, read modes, failover with/without pre-vote, catch-up with/without snapshots, groups 1/2/4 on one machine. Not yet: open-loop latency curves, migration impact, multi-Raft overhead, JMH micro-benchmarks (docs/benchmarks.md).
24. **Coverage is reported, not enforced.** SPEC §9.9 targets ≥ 85% line coverage on raft/, kv/, shard/; the nightly job publishes the JaCoCo report but doesn't fail below a threshold.
