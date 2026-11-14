# RaftKV — A Sharded, Fault-Tolerant Distributed Key-Value Store in Core Java
### Full build specification v2 (hand this file to Claude / Claude Code)

---

## 0. How to use this spec with Claude

Save this file as `SPEC.md` in an empty folder, open Claude Code there (or paste it into Claude), and say:

> "Read SPEC.md. We are building this project milestone by milestone. Start with Milestone 1 only. Follow the module layout, interfaces, determinism contract and testing rules exactly. After each milestone, stop, show me the test results (including seed counts), and explain the design decisions and the relevant part of the Raft paper / thesis so I can learn them. Then quiz me with 3 'what happens if…' questions before we move on."

Rules for the build (tell Claude to follow these):

1. **One milestone at a time.** Do not start the next one until every test of the current one passes, including the randomized ones run for the seed count that milestone specifies, **and** every bug in the bug-injection suite (§9.6) that applies so far gets caught.
2. **Core Java 21 only** for the system: `java.nio`, `java.util.concurrent`, virtual threads, records, sealed interfaces, pattern-matching `switch`, `java.util.zip.CRC32C`, `java.nio.channels.FileChannel`. **No** Spring, Netty, gRPC, Akka, Atomix, Ratis, Copycat, Jackson, Guava, Lombok, or any Raft/RPC/serialization/hashing library.
   - Allowed for **tests and benchmarks only**: JUnit 5, AssertJ, jqwik (property-based testing), JMH (micro-benchmarks).
   - Build tool: Maven or Gradle (plain; only the test, shade, JMH and coverage plugins).
3. **The Raft paper and thesis are the source of truth.** Every rule in Figure 2 of *"In Search of an Understandable Consensus Algorithm (Extended Version)"* (Ongaro & Ousterhout) maps to a specific, commented line of code, e.g. `// Fig.2 AppendEntries rule 3`. Thesis-only rules cite the thesis section, e.g. `// Thesis §4.1 single-server change`.
4. **Determinism contract** (§3.2): time, randomness, scheduling, network **and disk** are injected (`Clock`, seeded `Random`, `Scheduler`, `Transport`, `Disk`). The whole multi-node, multi-group cluster must be able to run on **one thread** in simulation, so any failing run can be replayed exactly from its seed.
5. **I (the human) must understand every line.** Claude explains each component after writing it; I must be able to draw the protocol on a whiteboard and answer "what happens if…" questions.
6. **No silent shortcuts.** If Claude simplifies something relative to this spec, it writes the simplification into `docs/design.md` under "Deviations" with the reason.

---

## 0.1 What changed from v1 (and why)

| Area | v1 | v2 |
|---|---|---|
| Sharding | Stretch afterthought | **Core milestones 9–10**: multi-Raft hosting, ShardController group, pull-based migration with GC, shard-aware client, sharded chaos tests |
| Disk faults | "Crash keeps disk" — lost-fsync bugs invisible | **SimulatedDisk** loses / tears unsynced writes on crash, so missing fsyncs are caught |
| Test strength | Tests only | **Bug-injection suite**: ~15 deliberate Raft/shard bugs; CI proves the tests catch every one |
| Exactly-once | `lastSeq` per client (breaks with an async/pipelined client) | Thesis §6.3 / RIFL-style: cache per `(clientId, seq)` + client-sent ack watermark; sessions are **per shard** so they migrate with the data |
| Reads | ReadIndex/lease described loosely | Added the missing preconditions: current-term commit before ReadIndex; leader stickiness + check-quorum before leases |
| Membership | Single-server changes | Plus the **known single-server-change bug fix** (no config change until a current-term entry is committed) |
| Persistence | fsync before reply | Plus thesis §10.2.1: leader writes to disk in parallel with replication but counts itself in the quorum only after its own fsync |
| Wire protocol | One group per connection | `groupId` in every frame; one TCP connection multiplexes all Raft groups between two processes |
| Linearizability | Per-key checking | Same, with an explanation of *why* that is valid (locality, Herlihy & Wing) and bounds that keep checking tractable |
| Testing variety | Fixed fault mix | **Swarm testing**: each seed randomly picks which fault types are on, which finds more bugs than turning everything on |
| Formal methods | — | Optional TLA+/PlusCal model of shard migration, model-checked with TLC |

---

## 1. Goals and non-goals

### Goals
- A **sharded** key-value store: the keyspace is split into fixed shards spread across multiple **replica groups**, each group a 3–5 node Raft cluster. A small **ShardController** Raft group decides which group owns which shard.
- Each group stays available and consistent as long as a **majority** of its nodes are up; shards keep moving between groups while clients keep reading and writing.
- Full **Raft consensus**: leader election, pre-vote, check-quorum, log replication, persistence, snapshots/log compaction, membership changes, leadership transfer.
- **Linearizable** single-key operations, `GET`, `PUT`, `DELETE`, `CAS`, with **exactly-once** semantics for client retries, still correct while shards migrate.
- Custom **binary RPC protocol over TCP** built with `java.nio`, multiplexing many Raft groups over one connection.
- A **deterministic simulation and chaos harness** (crashes, partitions, drops, delays, reorders, duplicates, lost unsynced disk writes, shard moves) that **proves** histories are linearizable, plus a bug-injection suite that proves the harness has teeth.
- Benchmarks (throughput, latency, failover time, migration impact, scale-out) and a README with honest design trade-offs.

### Non-goals
- Not a production database: no SQL, no secondary indexes. Multi-key transactions only as the 2PC stretch (§6.8).
- No Byzantine fault tolerance (nodes crash, they do not lie).
- No authentication/TLS in v1 (future work).
- No automatic load-based rebalancing or shard splitting in the core (stretch §6.9).

---

## 2. Architecture overview

```
                     ┌───────────────────────────────────────────────┐
  Clients ──────────▶│  RaftKV client library                        │
  (CLI / app)        │  key → shard → group → leader; config cache;  │
                     │  retries; clientId + seq + ack watermark      │
                     └───────┬──────────────────────────┬────────────┘
                             │ Query(config)            │ KV ops
                             ▼                          ▼
              ┌───────────────────────────┐   ┌──────────────────────────────────────┐
              │ ShardController (group 0) │   │ Replica groups (group 1..G)          │
              │ Raft group, 3 nodes       │   │ each a Raft group of 3–5 nodes       │
              │ configs: shard → group    │◀──│ poll config, migrate shards between  │
              │ Join / Leave / Move/Query │   │ groups (FetchShard / DeleteShard)    │
              └───────────────────────────┘   └──────────────────────────────────────┘

 Physical view: each node *process* hosts replicas of several groups (multi-Raft).

  Process A                    Process B                    Process C
 ┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
 │ g0 replica (ctrl)    │    │ g0 replica (ctrl)    │    │ g0 replica (ctrl)    │
 │ g1 replica           │    │ g1 replica           │    │ g1 replica           │
 │ g2 replica           │    │ g2 replica           │    │ g2 replica           │
 │ ─────────────────────│    │ ─────────────────────│    │ ─────────────────────│
 │ shared NioTransport  │◀══▶│ shared NioTransport  │◀══▶│ shared NioTransport  │
 └──────────────────────┘    └──────────────────────┘    └──────────────────────┘
        (one TCP connection per process pair; frames carry groupId)

 Inside one group replica:
 ┌──────────────────────────────────────────────────────────────────┐
 │ Shard layer   — shard status table, config processing, migration │
 ├──────────────────────────────────────────────────────────────────┤
 │ KV server     — per-shard data + session tables, read path       │
 ├──────────────────────────────────────────────────────────────────┤
 │ Raft core     — roles, timers, log, commitIndex, snapshots,      │
 │                 membership; single-threaded event loop           │
 ├──────────────────────────────────────────────────────────────────┤
 │ Persistence   — RaftLog (segmented WAL), MetaStore, SnapshotStore│
 │                 all on top of a Disk interface                   │
 ├──────────────────────────────────────────────────────────────────┤
 │ Transport / Disk / Clock / Scheduler — real or simulated         │
 └──────────────────────────────────────────────────────────────────┘
```

### Repo / package layout

```
raftkv/
├── pom.xml (or build.gradle)
├── README.md
├── SPEC.md                               # this file
├── docs/
│   ├── design.md                         # design doc, diagrams, trade-offs, Deviations
│   ├── sharding.md                       # migration protocol, state diagram, edge cases
│   ├── testing.md                        # simulation, invariants, checker, bug suite
│   ├── benchmarks.md
│   └── tla/ShardMigration.tla            # optional formal model
├── src/main/java/dev/<you>/raftkv/
│   ├── Main.java                         # start a node process from config
│   ├── config/
│   │   ├── NodeConfig.java               # node id, host, ports, data dir, timeouts
│   │   └── ClusterBootstrap.java         # initial groups and members
│   ├── common/
│   │   ├── NodeId.java, GroupId.java, ShardId.java (records)
│   │   ├── Clock.java / SystemClock.java / ManualClock.java
│   │   ├── Scheduler.java                # schedule(delay, task); real + simulated
│   │   ├── Hashing.java                  # hand-written 64-bit hash (FNV-1a or Murmur3)
│   │   └── Errors.java                   # sealed error hierarchy (§4.5)
│   ├── codec/
│   │   ├── BinaryWriter.java / BinaryReader.java
│   │   ├── Message.java (sealed interface)
│   │   └── MessageCodec.java
│   ├── io/
│   │   ├── Disk.java                     # open, write, force, rename, fsyncDir, list, delete
│   │   ├── RealDisk.java                 # FileChannel-backed
│   │   └── sim/SimulatedDisk.java        # models unsynced data, torn writes on crash
│   ├── transport/
│   │   ├── Transport.java
│   │   ├── NioTransport.java             # Selector-based TCP, framing, reconnect, groupId mux
│   │   ├── Connection.java
│   │   └── sim/SimulatedNetwork.java
│   ├── raft/
│   │   ├── RaftNode.java                 # one group replica: roles, handlers, timers
│   │   ├── RaftHost.java                 # hosts many RaftNodes in one process; routes by groupId
│   │   ├── Role.java                     # FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER, LEARNER
│   │   ├── RaftState.java / LeaderState.java
│   │   ├── rpc/ RequestVote(+Reply, preVote flag), AppendEntries(+Reply),
│   │   │        InstallSnapshot(+Reply), TimeoutNow
│   │   ├── log/ LogEntry, RaftLog, SegmentedFileLog, InMemoryLog
│   │   ├── storage/ MetaStore, SnapshotStore
│   │   ├── membership/ ConfigurationManager
│   │   └── StateMachine.java             # apply, snapshot, restore
│   ├── kv/
│   │   ├── KVCommand.java (sealed: Put, Delete, Cas, Get, NoOp)
│   │   ├── ShardData.java                # sorted map + SessionTable for ONE shard
│   │   ├── SessionTable.java             # (clientId, seq) → result, ack watermark
│   │   ├── KVServer.java                 # client RPCs for one group
│   │   └── ReadIndex.java
│   ├── shard/
│   │   ├── ShardConfig.java (record)     # num, shard[] → gid, groups → servers
│   │   ├── ShardController.java          # StateMachine for group 0
│   │   ├── Rebalancer.java               # deterministic minimal-move algorithm
│   │   ├── ShardKVStateMachine.java      # StateMachine for data groups
│   │   ├── ShardStatus.java              # SERVING, PULLING, BEPULLING, GCING, NOT_OWNED
│   │   ├── ConfigPoller.java             # leader-only loop: query controller, propose next config
│   │   ├── ShardMigrator.java            # leader-only loop: FetchShard / DeleteShard
│   │   └── rpc/ FetchShard(+Reply), DeleteShard(+Reply)
│   ├── client/
│   │   ├── RaftKVClient.java
│   │   └── cli/RaftKVCli.java
│   ├── metrics/ Metrics.java, Histogram.java (HdrHistogram-lite)
│   └── testing/BugSwitches.java          # compile-time-false flags for the bug suite (§9.6)
├── src/test/java/...
│   ├── unit/                             # codec, log, meta store, hashing, rebalancer
│   ├── sim/                              # Simulation, SimulatedCluster, Nemesis, Swarm
│   ├── raft/                             # election, replication, persistence, snapshots, membership
│   ├── shard/                            # controller, migration, GC, sharded chaos
│   ├── linearizability/                  # history recorder + checker
│   ├── bugs/                             # bug-injection suite
│   └── integration/                      # real processes over real TCP
├── bench/                                # JMH + load generator
├── scripts/
│   ├── run-cluster.sh                    # start N local processes hosting G groups
│   ├── kill-node.sh / partition.sh
│   └── plot.py                           # charts for README (optional helper)
└── .github/workflows/ci.yml
```

---

## 3. Raft core (`raft/`)

### 3.1 State (exactly Figure 2)

**Persistent on all servers** (durable before replying to any RPC that depends on it):
- `currentTerm` — latest term seen
- `votedFor` — candidate voted for in current term (or null)
- `log[]` — entries `(index, term, type, data)`

**Volatile on all servers:** `commitIndex`, `lastApplied`

**Volatile on leaders (reset after election):** `nextIndex[peer]`, `matchIndex[peer]`

### 3.2 Event loop and determinism contract
- Each `RaftNode` is a **single-threaded event loop**: one logical thread processes all its events (RPCs, timer ticks, client proposals, I/O completions, apply results). No locks inside the Raft core. Explain the trade-off in the README: simplicity and reproducibility vs one core per group (multi-Raft gives back parallelism across groups).
- Disk fsync and network sends run on I/O executors and **post completion events back** into the loop. A reply that depends on durable state (vote grant, AppendEntries success) is sent **only from the fsync-completion event**, never before.
- The Raft core never creates threads, never calls `System.currentTimeMillis`, `new Random()`, `Thread.sleep`, or touches files directly. It receives `Clock`, `Random`, `Scheduler`, `Transport` and `Disk`.
- In simulation, one `Simulation` object owns a priority queue of events ordered by `(virtualTime, seededTieBreaker)` and runs **every node of every group** on one thread. Same seed ⇒ same execution, byte for byte. Add a test that runs a seed twice and compares the full event trace hash.
- Collections whose iteration order affects behavior (peer lists, groups, shards) are `TreeMap`/sorted lists, never `HashMap` iteration.

### 3.3 Roles and transitions

```
                 timeout                      pre-vote majority
  ┌──────────┐ ─────────────▶ ┌──────────────┐ ─────────────────▶ ┌───────────┐ majority ┌────────┐
  │ FOLLOWER │                │ PRE_CANDIDATE│                    │ CANDIDATE │ ───────▶ │ LEADER │
  └──────────┘ ◀───────────── └──────────────┘                    └───────────┘          └────────┘
       ▲   ▲    hears leader / higher term            timeout → new pre-vote                  │
       │   └──────────────────────────── discovers higher term ───────────────────────────────┘
       │                                  (or loses quorum contact: check-quorum)
  ┌──────────┐
  │ LEARNER  │  non-voting; receives entries, never times out into an election
  └──────────┘
```

### 3.4 Leader election
- Election timeout randomized in `[150ms, 300ms]` (configurable); heartbeat every 50ms.
- **Pre-vote** (thesis §9.6): on timeout, become PRE_CANDIDATE and send `RequestVote(preVote=true, term=currentTerm+1)` **without** incrementing or persisting the term. A voter answers yes only if the candidate's log is up to date **and** it has not heard from a current leader within the minimum election timeout. Only with a pre-vote majority does the node become CANDIDATE: increment term, vote for self, persist, send real `RequestVote`.
- **Grant a real vote** only if (a) `term >= currentTerm`, (b) not voted for someone else this term, (c) candidate's log is at least as up to date (last log term, then last log index — §5.4.1), and (d) **leader stickiness**: this node has not heard from a valid leader within the minimum election timeout (thesis §4.2.3; ignored when the request carries the `TimeoutNow` transfer flag). Rule (d) is what makes lease reads safe (§4.4) and stops removed servers from disrupting the cluster.
- On becoming leader: initialize `nextIndex = lastLogIndex+1`, `matchIndex = 0`, and **immediately append a no-op entry** in the new term (§5.4.2 / §8). Without it, entries from earlier terms cannot commit, and ReadIndex cannot be served.
- **Check-quorum:** a leader that has not received a response from a majority (counting itself) within one election timeout steps down to FOLLOWER.

### 3.5 Log replication
- `AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)`.
- Follower: reply false if `term < currentTerm` (Fig.2 rule 1) or no entry at `prevLogIndex` with `prevLogTerm` (rule 2). If an existing entry conflicts with a new one (same index, different term), delete it and **all that follow** (rule 3) — but **never truncate on a matching prefix** (a stale, reordered AppendEntries must not delete newer entries). Append new entries (rule 4). Set `commitIndex = min(leaderCommit, index of last new entry)` (rule 5).
- **Fast backtracking** (exact algorithm, §5.3):
  - Follower log too short → `conflictIndex = lastLogIndex+1`, `conflictTerm = none`.
  - Otherwise → `conflictTerm = log[prevLogIndex].term`, `conflictIndex =` first index whose term is `conflictTerm`.
  - Leader: if it has entries with `conflictTerm`, `nextIndex =` (its last index with that term) + 1; else `nextIndex = conflictIndex`.
- **Commit rule:** leader sets `commitIndex = N` when a majority has `matchIndex >= N` **and** `log[N].term == currentTerm` (§5.4.2). The Figure 8 scenario must be reproduced as a scripted test, and its bug variant must be caught (§9.6).
- **Leader's own durability** (thesis §10.2.1): the leader may send entries to followers in parallel with its own disk write, but its own `matchIndex` only advances when its local fsync completes.
- **Pipelining & batching:** bounded in-flight window per follower (e.g. 8 RPCs); batch entries up to a byte limit (e.g. 256 KB) per RPC. On any rejection, reset the pipeline to the backtracked `nextIndex`. Benchmark on vs off.
- **Flow control:** cap in-flight bytes per follower; a slow follower never makes the leader buffer without limit.

### 3.6 Applying to the state machine
- Entries in `(lastApplied, commitIndex]` are delivered in order to `StateMachine.apply(entry)`. Applying must be **deterministic**: same entries ⇒ same state on every replica (no wall clock, no randomness, no `HashMap` iteration).
- Waiting client requests: `index → (expectedTerm, CompletableFuture<Result>)`. When index `i` is applied, if `log[i].term != expectedTerm`, complete with `NotLeader`. On stepping down, fail all pending futures with `NotLeader`.

### 3.7 Persistence (`raft/log`, `raft/storage`, `io/`)
**Disk abstraction:** all file I/O goes through `Disk`. `RealDisk` uses `FileChannel`; `SimulatedDisk` keeps, per file, a durable image and a list of unsynced writes. On `crash(node)`, each unsynced write is independently **kept, dropped, or torn** (prefix kept) by the seeded RNG; renames not followed by a directory fsync may be undone. This is what makes the fsync rules below testable.

**MetaStore** (`currentTerm`, `votedFor`): write `meta.tmp`, `force`, `rename` to `meta`, `fsyncDir`. Explain what goes wrong if any step is skipped (torn file, rename lost on crash, stale vote ⇒ double vote ⇒ two leaders in one term).

**SegmentedFileLog:**
- Segment files `log-<firstIndex>.seg` (e.g. 64 MB).
- Record: `[length:4][crc32c:4][index:8][term:8][type:1][data:N]`, CRC over everything after the CRC field.
- In-memory index `index → (segment, offset)`, LRU cache of recent entries.
- `append(entries)` → write, then `force(false)`; concurrent appends coalesce into one fsync (**group commit**).
- `truncateSuffix(fromIndex)`: truncate the file and fsync before acknowledging anything new.
- `truncatePrefix(upToIndex)`: delete whole old segments after a snapshot is durable.
- Startup: scan segments, verify CRCs and index continuity, **truncate a torn/corrupt tail** in the last segment. Corruption in the middle (not the tail) is a fatal error, not something to truncate silently. Test both.

### 3.8 Snapshots & log compaction (§7)
- Trigger when the log exceeds a threshold (e.g. 10k entries or 64 MB).
- Snapshot at `lastApplied`: `lastIncludedIndex`, `lastIncludedTerm`, latest cluster membership config, full state machine state (for data groups: every shard's data, session table and status, plus the current and pending shard configs), trailing CRC.
- Taken **in the background**: capture an immutable view on the event loop (persistent/copy-on-write structures, or a `ConcurrentSkipListMap` snapshot iterator plus version counters), then serialize on an I/O thread. Writes never pause.
- Write to `snap.tmp` → force → rename → fsyncDir → then `truncatePrefix(lastIncludedIndex)`.
- **InstallSnapshot** (Fig. 13): chunked by offset; follower writes chunks to a temp file; on the last chunk, if its log has an entry matching `(lastIncludedIndex, lastIncludedTerm)` it keeps the entries after it, otherwise discards the whole log; then restores the state machine and sets `commitIndex = lastApplied = lastIncludedIndex`. Ignore snapshots older than the current `lastApplied`.

### 3.9 Membership changes (thesis ch. 4)
- **Single-server changes** only: add or remove one voter at a time.
- A configuration entry takes effect on a server **as soon as it is appended** to that server's log (not when committed); if it is later truncated, revert to the previous config.
- **Safety fix** (Ongaro, raft-dev 2015): a leader must not append a config change until it has **committed an entry from its current term** (the no-op), and must not start a second change until the previous one is committed. Add a test reproducing the bug without this rule, and a bug switch for it.
- New servers join as **LEARNER**, receive the log/snapshot, and are promoted when caught up (e.g. within one round of the leader's last index for N consecutive rounds).
- Removing the leader: the leader keeps managing the cluster while the change commits, then steps down (it doesn't count itself in the new config's majority).
- **Leadership transfer:** leader stops accepting proposals, catches the target up, sends `TimeoutNow`; the target starts an election immediately (skipping pre-vote, with the transfer flag so stickiness doesn't block it). Abort after one election timeout.
- Admin commands: `addServer`, `removeServer`, `promoteLearner`, `transferLeadership`.

### 3.10 Raft pitfalls checklist (Claude must confirm each in code review)
- [ ] A higher term in **any** RPC or reply converts the node to follower and persists the term **before** acting on it.
- [ ] Replies from older terms are ignored (stale `RequestVoteReply`, `AppendEntriesReply`).
- [ ] The election timer resets only on: granting a vote, valid AppendEntries from the current leader, starting an election. **Not** on rejecting a vote.
- [ ] A follower never truncates its log because of a stale or reordered AppendEntries whose entries already match.
- [ ] `nextIndex` never goes below 1 (or below `snapshotIndex+1`; send InstallSnapshot instead).
- [ ] `commitIndex` never decreases; `lastApplied` never passes `commitIndex`.
- [ ] Applying happens in the event loop's order; client futures are never completed from a different term's entry.
- [ ] Nothing is applied that came from a snapshot older than `lastApplied`.

---

## 4. KV layer (`kv/`)

### 4.1 Operations

| Operation | Semantics |
|---|---|
| `PUT key value` | set value; returns previous value |
| `GET key` | linearizable read |
| `DELETE key` | remove key; returns whether it existed |
| `CAS key expected new` | set only if the current value equals `expected` (`expected = absent` allowed); returns success + current value |
| `SCAN start end limit` | stretch: ordered range read **within one shard** (hash sharding scatters ranges — see §6.9) |

Keys and values are byte arrays (max key 1 KB, max value 1 MB in v1).

### 4.2 Exactly-once semantics (§8, thesis §6.3)
- Each client session has a `clientId` (registered through the log, or a 128-bit random UUID) and a per-client increasing `seq`.
- Every request carries `(clientId, seq, ackedBelow)`, where `ackedBelow` is the lowest `seq` the client has **not yet received a reply for**. The server may discard cached results for `seq < ackedBelow`.
- `SessionTable`: `clientId → {ackedBelow, results: seq → result}`. On apply: if a result for `seq` is cached, return it without re-applying; if `seq < ackedBelow`, return `SessionExpired`/stale error; otherwise apply and cache. This supports **many outstanding requests per client** (the async API), which a single `lastSeq` cannot.
- **Sessions are per shard** (part of `ShardData`), so the dedup state for a key always travels with that key during migration.
- **Expiry:** the leader stamps each entry with a timestamp `ts = max(prevTs, leaderClock.now())`. Sessions idle for longer than `sessionTimeout` (measured in **log timestamps**, never local clocks) are evicted during apply. Explain why: every replica must evict at the same log index, or replicas diverge.

### 4.3 State machine for a data group
- `ShardKVStateMachine`: `shardId → ShardData{ TreeMap<bytes,bytes> or ConcurrentSkipListMap, SessionTable, status }` plus `currentConfig`, `prevConfig`.
- Every client command is re-validated **at apply time** (not just when received): the key's shard must be owned and `SERVING`/`GCING` in the config the group has applied by then. Otherwise the result is `WrongGroup`.

### 4.4 Linearizable reads — implement and benchmark three modes
1. **Log read:** `GET` goes through the log. Simple, slow, always correct.
2. **ReadIndex** (thesis §6.4): leader checks that it has committed an entry **in its current term** (wait for the no-op otherwise); records `readIndex = commitIndex`; confirms it is still leader with a heartbeat round acknowledged by a majority; waits until `lastApplied >= readIndex`; reads locally. Batch many concurrent reads onto one heartbeat round.
3. **Lease read** (thesis §6.4.1): after a heartbeat round started at time `t` gets majority acks, the leader holds a lease until `t + electionTimeoutMin / (1 + maxClockDriftRate)`. Correct **only** because of leader stickiness (§3.4 d) and check-quorum; document the clock-drift assumption and add a simulation test where a clock skewed beyond the bound breaks linearizability (to prove the checker notices).
- Optional: **follower reads**: follower asks the leader for a ReadIndex, waits for `lastApplied >= readIndex`, reads locally.

### 4.5 Error model
Sealed hierarchy returned over the wire: `NotLeader(leaderHint?)`, `WrongGroup(configNum)`, `Timeout`, `SessionExpired`, `ShardNotReady` (shard is PULLING — retry same group), `Overloaded` (back-pressure), `InvalidRequest`. The client's retry policy depends on the type (§7).

---

## 5. Networking & protocol (`transport/`, `codec/`)

### 5.1 Wire format
- Handshake: magic `RKV1`, protocol version byte, sender `NodeId`.
- Frame: `[length:4][type:2][groupId:4][correlationId:8][flags:1][payload:N][crc32c:4]`.
- Hand-written serialization (`BinaryWriter`/`BinaryReader` on `ByteBuffer`), length-prefixed byte arrays, varints optional. **No Java serialization** (explain: deserialization gadgets, speed, no schema evolution).
- Messages are `record`s implementing `sealed interface Message`; the codec uses an exhaustive pattern-matching `switch` (adding a message without codec support is a compile error).
- Max frame size enforced before allocating (a malicious or corrupt length must not cause an OOM).

### 5.2 NioTransport
- One `Selector` thread for all sockets; non-blocking `SocketChannel`s; per-connection write queue with high/low watermarks (back-pressure surfaces as `Overloaded`).
- **One connection per process pair** carries every group's traffic; `RaftHost` routes inbound frames by `groupId`.
- Persistent peer connections with **reconnect + exponential backoff with full jitter**.
- Request/response matching by `correlationId` with per-request timeouts → `CompletableFuture`; pending futures fail fast when the connection drops.
- Separate listener ports for peer RPCs and client RPCs.
- Stretch optimization: **coalesced heartbeats**: one heartbeat message per process pair per tick covering every group, as CockroachDB does. Measure CPU/network savings with 50+ groups.
- Property test: random messages → encode → split into random-sized chunks (TCP fragmentation) → decode == original; bit-flipped frames are rejected by CRC.

### 5.3 SimulatedNetwork (the most important class in the test suite)
- In-memory, driven by the `Simulation` event queue and seeded `Random` → fully deterministic.
- Fault controls: `drop(p)`, `delay(min,max)`, `duplicate(p)`, `reorder`, `partition(setA, setB)`, asymmetric `oneWay(a→b)`, `isolate(node)`, `heal()`, `crash(node)` (loses volatile state and unsynced disk writes via SimulatedDisk), `restart(node)`, `clockSkew(node, rate)`.
- Every failing test prints its seed and fault profile: `Re-run with -Draftkv.seed=8812734 -Draftkv.profile=swarm`.

---

## 6. Sharding (`shard/`) — core

Modeled on MIT 6.5840 ShardKV (including both "challenge" requirements: garbage collection, and serving unaffected shards during migration).

### 6.1 Partitioning
- **N fixed shards** (default 64, constant for the cluster's lifetime): `shard = hash64(keyBytes) mod N` with the hand-written `Hashing` (FNV-1a 64 or Murmur3 64). Never `Arrays.hashCode`/`Object.hashCode` (not a stable cross-version contract, poor distribution).
- Fixed virtual shards mean a key never changes shard; only shard → group ownership changes. Explain the trade-off vs consistent hashing and vs range partitioning (§6.9).

### 6.2 Multi-Raft hosting
- A node process runs a `RaftHost` with several `RaftNode`s: group 0 (controller) and any number of data groups. Each group has its own event loop (a platform thread or single-threaded executor in production; all on the one simulation thread in tests), its own log directory and its own snapshots.
- Trade-off to document: per-group WAL (simple, isolated, more fsyncs) vs a shared WAL across groups (TiKV raft-engine style: fewer fsyncs, harder compaction). Implement per-group; discuss shared WAL as future work.

### 6.3 ShardController (group 0)
- State machine over a list of configs. `ShardConfig(num, int[] shardToGid, SortedMap<gid, List<server>> groups)`. Config 0 has no groups and every shard on gid 0 (unassigned).
- Operations (through the log, with sessions for exactly-once):
  - `Join(Map<gid, servers>)` → new config, rebalance.
  - `Leave(List<gid>)` → new config; their shards move to the remaining groups.
  - `Move(shard, gid)` → new config moving just that shard (no rebalance).
  - `Query(num)` → config `num`, or latest if `num = -1` (ReadIndex read).
- **Rebalancer** (pure function, property-tested with jqwik): given the current assignment and group set, produce an assignment where every group has `⌊N/G⌋` or `⌈N/G⌉` shards, **moving the minimum number of shards**, fully deterministic (sort gids; the extra shards go to the lowest gids; unassigned and departing groups' shards are reassigned first). Properties: balanced, minimal moves, deterministic, no shard on a departed gid.

### 6.4 Shard state per data group
Each data group tracks, for each shard, a status:

| Status | Meaning | Serves client ops? |
|---|---|---|
| `NOT_OWNED` | not ours in the current config | no → `WrongGroup` |
| `SERVING` | ours, data present | yes |
| `PULLING` | ours in the current config, data still at the previous owner | no → `ShardNotReady` |
| `BEPULLING` | ours in the previous config, not now; we hold frozen data for the new owner | no → `WrongGroup` |
| `GCING` | ours, data installed, previous owner not yet told to delete | yes |

```
             config makes it ours (prev owner ≠ none)        InsertShard applied
 NOT_OWNED ───────────────────────────────────────▶ PULLING ─────────────────▶ GCING
     ▲  │   config makes it ours (fresh / was gid 0)                              │ DeleteShard acked
     │  └────────────────────────────────────────────────────────────▶ SERVING ◀──┘
     │                                                                    │
     │  DeleteShard applied                config moves it away           │
     └──────────────────── BEPULLING ◀────────────────────────────────────┘
```

### 6.5 Configuration processing (one config at a time)
- The **leader's** `ConfigPoller` polls the controller (every ~100ms) for config `current.num + 1` and proposes it as a `ConfigChange(config)` log entry **only if every shard is in `SERVING` or `NOT_OWNED`** (the previous migration is finished). Configs are never skipped: going from 5 to 7 directly could lose shards that moved twice.
- On applying `ConfigChange(num)` (ignore if `num != current.num + 1`): set `prevConfig = current`, `current = config`, and recompute statuses: shards gained → `PULLING` (or `SERVING` if the previous owner was gid 0); shards lost → `BEPULLING`; others unchanged. Because this happens at one log index, **every replica switches at the same point in the operation order**: no operation is applied by both the old and the new owner.
- Shards unaffected by the change keep serving the whole time.

### 6.6 Migration protocol (pull-based) and garbage collection
1. **Fetch.** The new owner's leader `ShardMigrator` sends `FetchShard(configNum, shardIds)` to the servers of the previous owner group (from `prevConfig`). The previous owner answers **only if its `current.num >= configNum`** (it has applied the config that froze the shard); otherwise `NotReady`, and the puller retries. Any replica that has applied that config can answer, leader or not, because a `BEPULLING` shard is frozen and identical on all of them. The reply includes each shard's data **and its session table**.
2. **Install.** The puller proposes `InsertShard(configNum, shardId → ShardData)`. On apply: if `configNum == current.num` and status is `PULLING`, install data + sessions, set `GCING`; otherwise ignore (duplicate or stale — this makes insertion idempotent).
3. **GC.** For `GCING` shards, the leader sends `DeleteShard(configNum, shardIds)` to the previous owner. The previous owner proposes it through its log; on apply, if `configNum == current.num` and status is `BEPULLING`, delete the data and set `NOT_OWNED`. It replies `OK` once applied, **or if its config is already past `configNum`** (already handled).
4. **Finish.** On `OK`, the new owner proposes `GcDone(configNum, shardIds)`; on apply, `GCING → SERVING`. When all shards are settled the poller may fetch the next config.

Why pull, not push: the receiver knows when it is ready and drives retries; the sender stays passive and simple; crash recovery is "keep polling." Document the alternative.

### 6.7 Edge cases the tests must cover
- Previous owner's whole group is down mid-migration → the shard stays `PULLING` (unavailable) until it returns; unrelated shards keep serving. (Availability cost of strong consistency; say so in the README.)
- Leader of the pulling group crashes after `FetchShard` but before `InsertShard` commits → the new leader re-derives the work from shard statuses (all migration state lives in the replicated state machine, none in leader memory).
- Duplicate / reordered `FetchShard`, `InsertShard`, `DeleteShard` messages → idempotent by `configNum`.
- A group that `Leave`s still holds `BEPULLING` data → its servers must not be shut down until GC finishes (admin tool refuses or warns).
- Controller group unavailable → data groups keep serving their current config; only new moves are delayed.
- Snapshot taken mid-migration → snapshot includes statuses, `prevConfig`, and frozen shard data; a restart resumes the migration correctly.
- Client op whose key moves while the request is in flight → `WrongGroup` at apply time, client refreshes config and retries with the **same `seq`**; dedup state migrated with the shard prevents double-apply.
- Shard moves A → B → A across consecutive configs.

### 6.8 Cross-shard transactions (stretch)
- Two-Phase Commit where the **coordinator and every participant are Raft groups** (each decision is durable via its log, so a crashed coordinator does not leave participants blocked forever once its group recovers).
- Participants take per-key write intents/locks during PREPARE (logged), release on COMMIT/ABORT. Transaction records keyed by txnId; recovery resolves in-doubt transactions by asking the coordinator group.
- Interaction with migration: a shard with outstanding intents cannot be frozen until they are resolved (or the intents migrate with it — pick one and document it).
- Verify with a strict-serializability check on bank-transfer workloads (total balance invariant + an Elle-style anomaly check as further stretch).

### 6.9 Further stretch
- **Range partitioning + shard split/merge** (enables cross-shard `SCAN`, like CockroachDB/TiKV regions).
- **Load-based rebalancing** in the controller using per-shard QPS reported by groups.
- **Scatter-gather SCAN** across hash shards (results unordered across shards; document the consistency level: each shard is linearizable, the combined scan is not a snapshot).

### 6.10 Sharding invariants (checked continuously in simulation)
- **Single owner:** for every config num, at most one group has a shard in `SERVING`/`GCING` for that num.
- **No lost data:** the union of keys over the shards' current holders equals the model's expected keyset whenever the system is quiescent.
- **Monotonic configs:** every group's applied config num only increases, by exactly 1 at a time.
- **No double apply:** each `(clientId, seq)` is applied at most once across all groups and all time.

---

## 7. Client library & CLI (`client/`)

```java
try (var client = RaftKVClient.connect(List.of("localhost:7001", "localhost:7002", "localhost:7003"))) {
    client.put("user:42", "Asha");
    Optional<String> v = client.get("user:42");
    boolean ok = client.cas("counter", "5", "6");
    client.delete("user:42");
    CompletableFuture<Optional<byte[]>> f = client.getAsync(bytes("city"));
}
```
- Bootstrap from any node; fetch the latest `ShardConfig` from the controller; cache it.
- Routing: `key → shard → gid → servers`, with a cached leader per gid.
- Retry policy by error type: `NotLeader` → follow the hint, else rotate; `WrongGroup` → refresh config, reroute; `ShardNotReady` → back off, same group; `Timeout` → rotate servers, then refresh config. Exponential backoff with full jitter, overall deadline per op.
- **Same `seq` on every retry** (exactly-once), and `ackedBelow` maintained across concurrent async ops.

CLI:
```
$ ./raftkv-cli --cluster localhost:7001,localhost:7002,localhost:7003
raftkv> put city Hyderabad
OK (shard=17 group=2 leader=node-5, 3.1 ms)
raftkv> get city
"Hyderabad" (2.4 ms, read=readindex)
raftkv> status
group 0 (controller)  leader=node-1 term=4  config=12
group 1  node-2 LEADER term=7 commit=10422 applied=10422  shards=22 serving
group 2  node-5 LEADER term=3 commit=8811  applied=8811   shards=21 serving, 1 pulling
group 3  node-8 LEADER term=9 commit=9002  applied=9002   shards=21 serving
raftkv> shards
0:g1 1:g3 2:g2 ... 17:g2 ...
raftkv> admin join 4 node-10,node-11,node-12
raftkv> admin move 17 4
raftkv> admin leave 3
raftkv> admin transfer-leader 1 node-3
```

---

## 8. Observability
- Structured logs: `[node-2 g1 term=7 LEADER] committed index=10422`; per-group log level.
- Metrics (plain text on a separate TCP port, plus JMX): role/term/commit/applied per group, replication lag per follower, elections, RPC latency histograms, fsync latency, group-commit batch size, snapshot duration/size, **shard status counts, migration duration and bytes per shard, `WrongGroup` rate**.
- `status` admin RPC used by the CLI.
- Optional tiny **web dashboard** (`com.sun.net.httpserver.HttpServer` + one static HTML page): live grid of groups × nodes with roles, and a shard map that animates during migrations (great for the demo GIF).

---

## 9. Testing — the part that makes this project stand out

### 9.1 Test pyramid

| Layer | What to test |
|---|---|
| Codec | round-trip property tests, fragmentation, CRC rejection, oversize frames rejected |
| Disk / MetaStore / Log | crash at every step of write/force/rename/fsyncDir; torn tail truncation; mid-log corruption is fatal; truncateSuffix/Prefix; reopen |
| Election | one leader per term; re-election after leader crash; no election with a healthy leader; pre-vote stops a rejoining node from disrupting; split votes resolve; stickiness |
| Replication | catch-up; conflicting logs repaired; fast backtracking; stale-AppendEntries-no-truncate; **Figure 8 scenario** |
| Persistence | every node crashes at once (losing unsynced writes) and recovers with all committed data |
| Snapshots | InstallSnapshot to a lagging follower; restart from snapshot; snapshot under heavy load; snapshot mid-migration |
| Membership | add/remove under load; remove the leader; learner catch-up; the single-server-change bug scenario |
| KV | exactly-once with concurrent async retries; CAS; all three read modes linearizable; session expiry deterministic across replicas |
| Controller | rebalancer properties (jqwik); Join/Leave/Move/Query semantics; controller crash/restart |
| Sharding | migration under load; GC completes; unaffected shards serve during migration; every §6.7 edge case |
| Chaos | swarm-randomized faults + linearizability checker + invariants (§9.2–9.5) |
| Bug suite | each injected bug detected (§9.6) |
| Integration | real processes on localhost over real TCP; `kill -9`; Jepsen-lite (§9.7) |

### 9.2 Simulation harness
- `SimulatedCluster.builder().nodes(9).groups(3).shards(64).seed(s).build()`; `Nemesis` schedules faults; `Workload` drives clients; `run(Duration virtualTime)`.
- Virtual time makes a 60-second chaos scenario run in well under a second, so thousands of seeds fit in CI.
- On failure: print the seed, fault profile, and the last N events per node; write the full trace to `target/sim-traces/<seed>.log`.

### 9.3 Safety invariants checked after every simulated event
- **Election Safety:** at most one leader per term per group.
- **Log Matching:** same `(index, term)` ⇒ identical prefixes.
- **Leader Completeness:** a committed entry is in the logs of all later leaders.
- **State Machine Safety:** no two replicas apply different entries at the same index.
- **Leader Append-Only:** a leader never overwrites or deletes its own entries.
- **Commit durability:** an index once reported committed is never lost after any crash/restart sequence.
- Plus the **sharding invariants** of §6.10.

### 9.4 Linearizability checker (`test/linearizability/`)
- Clients record a history: `(clientId, op, key, args, invokeTime, returnTime, result | unknown)` in virtual time.
- **Wing & Gong with Lowe's memoization** (cache of `(linearized-set bitset, model state)`), Porcupine-style.
- **Per-key partitioning is valid** because linearizability is *local* (Herlihy & Wing): a history is linearizable iff each per-object sub-history is. This holds across shards and across migrations, since each key is one object. Explain this in `docs/testing.md`.
- Timed-out / unknown ops are open-ended (may take effect any time after invoke, or never).
- Worst case is exponential (the problem is NP-complete), so keep per-key histories bounded (≤ ~300 ops, ≤ ~10 concurrent clients per key) and log checker time per run.
- Output a text (stretch: HTML) timeline of the smallest failing per-key history.
- Self-test: feed the checker hand-built non-linearizable histories (stale read, lost write, double CAS) and assert it rejects them.

### 9.5 Swarm testing and seed budgets
- Each seed randomly enables a subset of fault types and picks intensities (swarm testing, Groce et al.): some runs only partition, some only crash, some only lose disk writes. This finds bugs a fixed "all faults on" mix hides.
- Budgets: per-PR CI 300 seeds (~minutes); nightly 10,000+ seeds; milestone gates as listed in §11.

### 9.6 Bug-injection suite (proves the tests have teeth)
`BugSwitches` holds `static final` flags that are constant `false` in production builds (the JIT removes the dead branches) and can be enabled only through a test-only build profile. For each bug, a test enables it and asserts the harness **fails within K seeds**:

| Bug | Expected catcher |
|---|---|
| `COMMIT_PREVIOUS_TERM_BY_COUNT` (Figure 8) | State Machine Safety / Figure 8 test |
| `NO_UPTODATE_CHECK_ON_VOTE` | Leader Completeness |
| `VOTEDFOR_NOT_PERSISTED` | Election Safety (after crash) |
| `REPLY_BEFORE_FSYNC` | Commit durability (SimulatedDisk) |
| `TRUNCATE_ON_STALE_APPEND` | Log Matching / linearizability |
| `RESET_TIMER_ON_REJECTED_VOTE` | liveness test (no leader within T) |
| `NO_NOOP_ON_ELECTION` | ReadIndex linearizability / liveness |
| `READINDEX_WITHOUT_HEARTBEAT` | linearizability under partition |
| `LEASE_WITHOUT_STICKINESS` | linearizability under partition |
| `CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT` | Election Safety in membership chaos |
| `SESSION_DEDUP_DISABLED` | No double apply / linearizability |
| `SESSIONS_NOT_MIGRATED` | No double apply across groups |
| `SERVE_WHILE_BEPULLING` | Single owner / linearizability |
| `SKIP_CONFIG_NUMBERS` | No lost data |
| `GC_BEFORE_INSERT_COMMITTED` | No lost data |
| `CHECK_OWNERSHIP_ON_RECEIVE_ONLY` | linearizability during migration |

CI publishes a table: bug → seeds to first detection. This table goes in the README.

### 9.7 Jepsen-lite against real processes
- Script launches 9 JVMs (controller + 2 data groups), a load generator records a history over real TCP, a nemesis thread `kill -9`s and restarts nodes, triggers `admin move` / `join` / `leave`, and (Linux, optional) uses `iptables`/`tc` for partitions and delay. The history goes to the same checker.

### 9.8 Optional formal model
- `docs/tla/ShardMigration.tla`: model two groups and a controller with configs, statuses, Fetch/Insert/Delete messages (lossy, duplicating). Model-check with TLC for the Single Owner and No Lost Data invariants on small bounds. Write down one design flaw the model found, or the bounds it verified.

### 9.9 Coverage & quality
- ≥ 85% line coverage on `raft/`, `kv/`, `shard/` (JaCoCo).
- CI (GitHub Actions): build, unit tests, simulation (300 seeds), bug suite, coverage report; nightly 10k-seed job; Java 21.

---

## 10. Benchmarks (`bench/`)

Publish in `docs/benchmarks.md` and the README with charts. State hardware, JVM flags, disk type, and whether nodes share a machine.
1. **Write throughput** for 3- and 5-node groups: batching/pipelining on/off, group commit on/off.
2. **Latency** p50/p99/p999 for PUT and GET vs offered load (open-loop load generator to avoid coordinated omission).
3. **Read modes:** log vs ReadIndex vs lease: throughput and latency.
4. **Failover:** leader `kill -9` → first successful write, over 100 trials; effect of election timeout settings and pre-vote.
5. **Recovery time** vs log size, with and without snapshots.
6. **Sharding scale-out:** throughput with 1, 2, 4, 8 groups. Be honest: on one machine you hit CPU/disk limits, so either use separate machines/containers with pinned CPUs or report it as "per-core scaling" with the caveat.
7. **Migration impact:** p99 latency of unaffected shards and of the moving shard during a `Move`; time and throughput to migrate a 100 MB shard.
8. **Multi-Raft overhead:** CPU and messages/s vs number of groups per process, with and without coalesced heartbeats (if implemented).
9. JMH: codec encode/decode, log append with group commit, hash function, rebalancer.

> Only put numbers on your resume that these benchmarks actually produced.

---

## 11. Milestones (give Claude one at a time)

| # | Milestone | Done when | Est. time |
|---|---|---|---|
| 1 | Skeleton, `Clock`/`Scheduler`/`Random` injection, codec (sealed messages), `Simulation` event loop, SimulatedNetwork basic delivery, determinism trace-hash test | codec property tests pass; same seed ⇒ same trace hash | 4–5 days |
| 2 | Leader election (RequestVote, randomized timeouts, terms, heartbeats, stickiness) on InMemoryLog | election tests pass 500 seeds incl. leader crash & partitions; election-related bugs in §9.6 caught | 1 week |
| 3 | Log replication, commit rule, apply to a dummy state machine, fast backtracking, invariant checker | Figure 8 test passes; invariants hold over 1,000 seeds; replication bugs caught | 1.5–2 weeks |
| 4 | `Disk` + SimulatedDisk, MetaStore, SegmentedFileLog (CRC, torn tail, group commit), crash/restart in simulation | all-nodes-crash test passes with lost unsynced writes; `REPLY_BEFORE_FSYNC` caught | 1 week |
| 5 | KV state machine (single group for now), sessions with ack watermark, deterministic expiry, client library (leader discovery, retries, async) | KV + concurrent duplicate-retry tests pass | 1 week |
| 6 | Linearizability checker (+ self-tests), workload generator, swarm chaos suite | 1,000 swarm runs linearizable; checker rejects all hand-built bad histories | 1.5 weeks |
| 7 | NioTransport (groupId mux), RaftHost, Main, config, CLI, run-cluster script, kill -9 integration test | 3 real processes survive kill -9 of the leader | 1.5 weeks |
| 8 | Snapshots, log compaction, InstallSnapshot | lagging follower catches up via snapshot; restart from snapshot; 1,000 chaos seeds still linearizable | 1.5 weeks |
| 9 | **Multi-Raft + ShardController**: multiple groups per process, controller state machine, Rebalancer (jqwik), shard-aware client routing (static config, no migration yet) | controller tests + rebalancer properties pass; KV works across 3 groups with fixed assignment | 1.5 weeks |
| 10 | **Shard migration + GC**: statuses, ConfigPoller, FetchShard/InsertShard/DeleteShard/GcDone, per-shard sessions, all §6.7 edge cases | 2,000 sharded swarm seeds with concurrent Join/Leave/Move stay linearizable; §6.10 invariants hold; shard bugs caught | 2–2.5 weeks |
| 11 | ReadIndex + lease reads, pre-vote, check-quorum, batching/pipelining, group commit tuning | read-mode benchmarks; still linearizable under chaos; read bugs caught | 1 week |
| 12 | Membership changes (learners, add/remove, leadership transfer, the safety fix) | 3→5→3 under load (in a data group, while shards migrate) passes chaos | 1 week |
| 13 | Metrics, dashboard, benchmarks, charts, Jepsen-lite, README, design/sharding/testing docs, CI with nightly job | polished repo + demo GIF (kill leader + live shard move) | 1.5 weeks |
| Stretch | 2PC cross-shard transactions; TLA+ model; coalesced heartbeats; range shards + split | pick 1–2 | 3–4 weeks |

Total: ~5–5.5 months part-time.
**Cut lines:** 1–7 is a solid Raft project. 1–10 (sharded, chaos-verified) is the target for a strong resume. 11–13 make it polished; stretch items make it rare.

---

## 12. README must contain
1. One-paragraph pitch, architecture diagram (logical + physical multi-Raft view), and a GIF: kill a leader, watch re-election, then move a shard live while the load generator keeps running.
2. Quickstart: build, `run-cluster.sh` (9 processes, 3 groups), CLI demo, Java client example.
3. Feature list mapped to Raft paper/thesis sections and to 6.5840 ShardKV requirements.
4. **Design decisions & trade-offs:** single-threaded event loop vs locks; multi-Raft per process; per-group WAL vs shared WAL; ReadIndex vs lease reads (and why stickiness matters); single-server membership vs joint consensus; pull vs push migration; fixed hash shards vs ranges; why pre-vote; why the no-op; clock drift; availability cost of a stuck migration.
5. **Testing approach:** deterministic simulation, SimulatedDisk, swarm testing, invariants, linearizability checker (and why per-key checking is valid), bug-injection table, seed counts, how to replay a seed.
6. Benchmark charts with real numbers and hardware.
7. Known limitations & future work.
8. References: Raft paper (extended); Ongaro's PhD thesis; Ongaro's raft-dev post on the single-server change bug; MIT 6.5840 labs 3–5; *Designing Data-Intensive Applications* ch. 5–9; etcd/raft and TiKV (multi-Raft, raft-engine) design docs; CockroachDB's coalesced heartbeats; Porcupine; Herlihy & Wing (1990); Lowe, "Testing for linearizability" (2017); Groce et al., "Swarm Testing" (2012); Jepsen analyses; FoundationDB's simulation-testing talk; RIFL (Lee et al., 2015).

---

## 13. Resume bullets (template — fill with YOUR measured numbers)

- **RaftKV — Sharded Distributed Key-Value Store (Core Java 21)** | GitHub link
  - Implemented Raft from scratch (election, pre-vote, log replication, snapshots, membership changes) plus a sharding layer of **G** Raft groups with a controller group and live, garbage-collected shard migration; custom NIO binary RPC multiplexing all groups — no frameworks.
  - Built a FoundationDB-style deterministic simulator (network partitions, crashes, lost unsynced disk writes) and a Porcupine-style linearizability checker; verified **N** randomized runs, and showed the suite catches **16/16** deliberately injected consensus and migration bugs.
  - Reached **X** ops/s and **Y** ms p99 on a 3-node group via batching, pipelining and group commit; scaled to **S×** throughput with 4 groups; median leader failover **Z** ms.
  - Delivered exactly-once semantics across shard migrations (per-shard session tables) and linearizable ReadIndex reads, cutting read latency **K×** vs log reads.

---

## 14. Interview questions you must be able to answer

**Raft fundamentals**
- Walk me through a leader election. Why are timeouts randomized? What if two candidates split the vote?
- Why must a voter check that the candidate's log is at least as up to date? What breaks without it?
- Explain the Figure 8 problem. Why can't a leader commit an entry from a previous term by counting replicas?
- Why does a new leader append a no-op entry? What two things depend on it?
- What must be persisted before replying to an RPC, and why? What if `votedFor` were not persisted?
- How does a follower with a conflicting log get repaired? Walk through fast backtracking.
- Why must a follower not truncate on a stale AppendEntries whose entries already match?
- What was the bug in single-server membership changes, and what rule fixes it?

**Correctness & consistency**
- What is linearizability? How does it differ from serializability, strict serializability, and eventual consistency?
- Compare log reads, ReadIndex and lease reads. Why does lease safety depend on leader stickiness and bounded drift?
- How do you guarantee a retried PUT isn't applied twice, even with many outstanding async requests per client?
- Why is checking linearizability per key valid? What's the checker's complexity and how do you keep it tractable?
- How do you know your tests are strong enough? (bug-injection suite)

**Sharding**
- Why fixed virtual shards instead of consistent hashing or ranges? What do you lose (range scans)?
- Walk me through moving shard 17 from group 1 to group 2 while clients write to it. Where exactly does ownership switch, and why can't both groups apply a write?
- Why must a group process configs one at a time without skipping?
- Why check shard ownership at apply time and not only when the request arrives?
- Why can a non-leader replica of the old owner safely answer `FetchShard`?
- How do sessions/exactly-once survive a migration?
- What happens if the old owner group is entirely down during migration? If the controller is down?
- How would you add cross-shard transactions? What does 2PC with Raft-backed participants fix compared to classic 2PC?

**Failures**
- 5 nodes split 2–3? 2–2–1? What about a one-way partition?
- What if a disk loses the last few unsynced writes? How did you test it?
- A partitioned old leader still thinks it's leader — can it serve stale reads? How do check-quorum and leases interact?
- A node rejoins after a long partition with a much higher term — what happens with and without pre-vote?

**Performance & design**
- Where is the bottleneck? (fsync, round-trips, single event loop per group.) How did batching, pipelining and group commit help?
- Why a single-threaded event loop per group? How does multi-Raft give back parallelism?
- Per-group WAL vs shared WAL: trade-offs?
- How do snapshots work without pausing writes?
- Raft vs Multi-Paxos vs ZAB vs primary-backup: trade-offs?
- How do etcd, CockroachDB, TiKV and Kafka (KRaft) use Raft? What would you change to run this in production?
