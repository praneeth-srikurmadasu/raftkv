# Milestone 3: Log replication explained

With a leader in place, the cluster can now do real work: the leader accepts commands, copies them into every follower's log, decides when a command is **committed** (safe forever), and every server applies committed commands to its state machine in the same order.

New and changed code:

```
raft/
├── RaftNode.java          + propose(), AppendEntries rules 2-5, fast backtracking,
│                            commit rule, no-op on election, apply loop
├── LeaderState.java       nextIndex[] / matchIndex[]
├── StateMachine.java      the replicated application (KV store from Milestone 5)
├── CommitResult.java      what propose() completes with
├── NotLeaderException.java
├── Bug.java               + 4 replication bugs
└── RaftConfig.java        + maxEntriesPerAppend
```

---

## 1. The life of a command

```
client ──propose("PUT x=1")──▶ LEADER (term 4)
                                 │ 1. append 7@t4 to own log
                                 │ 2. AppendEntries(prev=6@t4, entries=[7@t4], leaderCommit=6) ─▶ followers
                                 │                     follower: rule 2 check, append, reply success matchIndex=7
                                 │ 3. matchIndex: leader 7, n2 7, n3 6  → index 7 is on a majority,
                                 │    and log[7].term == 4 == currentTerm  → commitIndex = 7
                                 │ 4. apply 7 to the state machine → complete the client's future
                                 │ 5. next AppendEntries carries leaderCommit=7 → followers commit + apply 7
```

**Committed** means: stored on a majority, in a way that every future leader is guaranteed to have it (Leader Completeness). From that point it can never be lost or changed.

## 2. The follower's rules (Fig. 2, AppendEntries receiver)

| Rule | Meaning | Why |
|---|---|---|
| 1 | reject if `term < currentTerm` | stale leader; our term in the reply makes it step down |
| 2 | reject if no entry at `prevLogIndex` with `prevLogTerm` | the **consistency check**: only accept entries that directly follow a point where our log provably matches the leader's |
| 3 | on a conflict (same index, different term), delete it and everything after | those entries came from an old leader and were never committed |
| 4 | append entries not already in the log | duplicates are skipped, so the message is idempotent |
| 5 | `commitIndex = min(leaderCommit, index of last new entry)` | entries beyond what *this* message verified may be stale |

Two traps (each has a test):
- **Never truncate on a match.** Messages get delayed and reordered. A late, shorter AppendEntries contains entries we already have. If we truncated after `prevLogIndex` anyway, we would delete newer entries the leader may already count as committed. (`staleShorterMessageDoesNotTruncate`, bug `TRUNCATE_ON_STALE_APPEND`.)
- **Rule 5's "last new entry".** Our log may have a tail from an old leader beyond what this message checked. Committing up to our last index could apply those stale entries. (`commitIndexIsCappedByTheLastVerifiedEntry`, bug `COMMIT_INDEX_FROM_LAST_LOG_INDEX`.)

## 3. Log Matching: why rule 2 is enough

Raft guarantees: *if two logs have an entry with the same index and term, the logs are identical up to that index.* The proof is induction. A leader creates at most one entry per index in its term, so (index, term) pins down the content. And rule 2 means an entry is only accepted right after a matching predecessor. So one successful consistency check at `prevLogIndex` proves the follower's whole log up to there equals the leader's.

Our invariant checker uses exactly that induction: every (index, term) pair ever seen must always have the same content **and the same predecessor term**.

## 4. Repairing a follower: fast backtracking (§5.3)

When rule 2 fails, the leader must find where the logs agree. The naive way decrements `nextIndex` one entry per round trip. Instead the follower says what it has:

```
follower log too short              →  conflictIndex = its last index + 1, conflictTerm = 0
term at prevLogIndex doesn't match  →  conflictTerm = that term, conflictIndex = first index of that term

leader: if it has entries of conflictTerm → nextIndex = (its last index of that term) + 1
        otherwise                         → nextIndex = conflictIndex
```

Example from `fastBacktrackingSkipsAWholeConflictingTerm`:
```
leader:  1..10 @t1   11..20 @t3   21 @t4 (no-op)
stale:   1..10 @t1   11..60 @t2               ← 50 entries from a term-2 leader that never committed
probe prev=20@t3 → stale has 20@t2 → "conflictTerm 2, starts at 11"
leader has no t2 → nextIndex = 11 → probe prev=10@t1 matches → entries 11.. replace the t2 tail
```
That's one rejection instead of ten.

The leader only ever *lowers* `nextIndex` on a rejection (a rejection may be stale) and never below `matchIndex + 1`.

### Flow control: only progress sends more

After a success, the leader immediately sends the next batch if the follower is still behind. But messages overlap (proposals, heartbeats and continuations can all be in flight) and the network duplicates them. So the leader only continues on a reply that **moved `matchIndex` forward**, and only retries after a rejection that **lowered `nextIndex`**. A duplicate or stale reply teaches the leader nothing, so it sends nothing. The 20,000-seed stress run found what happens without this rule (§8).

## 5. The commit rule and Figure 8 (§5.4.2)

> **Fig.2 Leaders:** if there is an N > commitIndex such that a majority have `matchIndex ≥ N` **and `log[N].term == currentTerm`**, set commitIndex = N.

Why the term condition? `Figure8Test` plays the paper's scenario:

```
         index 1   2
S1, S2:     x@1  y2@2     ← term-2 leader S1 got y2 onto S2, then crashed
S3, S4:     x@1
S5:         x@1  y3@3     ← term-3 leader S5 wrote y3 locally, then crashed

S1 wins term 4 and copies y2 to S3 → y2 is on S1, S2, S3: a MAJORITY.
S1 crashes before its own term-4 entry spreads.
S5 can still win term 5: its last term (3) beats S3's (2) and S4's (1).
S5 overwrites index 2 with y3 everywhere.
```

If S1 had treated "y2 is on a majority" as committed, it could have told a client "done", and then y2 would vanish. So a leader only commits **by counting** entries from its own term. Older entries commit *indirectly*: once an entry of the current term is committed, Log Matching means everything before it is committed too.

The test plays this both ways: correct code never commits y2 and everyone ends with y3; with `COMMIT_PREVIOUS_TERM_BY_COUNT` the leader applies y2, S5's group later applies y3, and State Machine Safety fails.

## 6. The no-op entry (§8)

Because of Figure 8, a new leader can't commit entries from earlier terms until it commits something of its own. So it immediately appends an empty **no-op** entry. Without it, an earlier write could sit uncommitted until some client happens to write again. `newLeaderCommitsEntriesFromEarlierTermsViaItsNoop` checks it; `NO_NOOP_ON_ELECTION` is caught at seed 1. The no-op is also what ReadIndex (Milestone 11) waits for.

## 7. Applying, and what clients see

`applyCommitted()` feeds entries `lastApplied+1 .. commitIndex` to the state machine in order (no-ops and configuration entries are skipped). Each proposal waits in `pending[index]`:
- the entry at that index is applied **with the same term** → the future completes with the result;
- a different term ended up there, or the leader stepped down → `NotLeaderException`.

Careful: `NotLeaderException` after stepping down does **not** mean the command failed. It may still commit under the next leader. A retrying client must reuse its request id so the duplicate is dropped. That's Milestone 5's exactly-once sessions.

In this milestone the state machine and `commitIndex` are volatile: a restarted node re-applies its log from index 1. Snapshots (Milestone 8) remove that replay.

## 8. Testing

| Test | Proves |
|---|---|
| `FollowerAppendRulesTest` (9) | each receiver rule, including both traps, idempotency and no-op handling |
| `ReplicationTest` (9) | ordering, catch-up across 300 entries, survival across 4 leader crashes, lost writes on a deposed leader, no-op commits, fast backtracking, bounded catch-up traffic, single node |
| `Figure8Test` | the paper's scenario, correct and buggy |
| `ReplicationChaosTest` | **gate:** 1,000 chaos runs with a client writing throughout |

The chaos run checks after **every** event: Election Safety, one vote per term, monotonic terms, Log Matching, Leader Append-Only, committed entries never change, Leader Completeness, and (on every apply) State Machine Safety. At the end, after healing, every node must apply a final marker with identical logs and identical applied sequences, and **every acknowledged write must still be in the log at its index**.

### Bug detection

| Bug | Caught by | Detection |
|---|---|---|
| `TRUNCATE_ON_STALE_APPEND` | chaos | seed 1 |
| `VOTE_WITHOUT_LOG_CHECK` | chaos (Leader Completeness) | seed 1 |
| `NO_NOOP_ON_ELECTION` | earlier-term-entry liveness test | seed 1 |
| `COMMIT_PREVIOUS_TERM_BY_COUNT` | scripted Figure 8 | always |
| `COMMIT_INDEX_FROM_LAST_LOG_INDEX` | rule-level test | always |

### Three lessons from building the bug suite

1. **The first chaos client was too polite.** It only wrote to the newest leader, so stale leaders rarely accepted writes. The divergent, uncommitted log tails that Figure 8 depends on hardly ever formed, and the Figure 8 bug survived 300 seeds. Real clients often have stale leader information, so the client now writes to *any* node that believes it's leader. After that change, chaos found the bug at seed 190. A test suite has to model the world's bad behavior, not just the happy path.
2. **Rare interleavings need scripted tests too.** After the flow-control fix below changed the traffic pattern, random chaos no longer found the Figure 8 bug within 300 seeds. Nothing about the bug changed; random search is simply fragile for one specific sequence of crashes. That's why the bug suite relies on the scripted `Figure8Test`, which catches it every time.
3. **Some bugs are unreachable by chaos in a given design.** `COMMIT_INDEX_FROM_LAST_LOG_INDEX` only bites when a follower holds verified entries followed by a stale tail *and* a higher `leaderCommit` arrives. Our leader always ships pending entries with every message, which breaks up stale tails first, so chaos never produced that situation. Implementations that send empty heartbeats while entries are pending *are* exposed. The rule-level unit test catches it directly. Chaos and unit tests complement each other.

### What the 20,000-seed stress run found

Before shipping, I run the chaos test with 20,000 fresh random seeds. On the first attempt, two seeds made the JVM **run out of memory** (one took 86 seconds to die). The cause was **message amplification**. Every success reply from a lagging follower triggered another send, and a duplicated reply triggered two. Each duplicate started another chain of messages that never ended, so on a duplicating network with one-entry batches, in-flight messages grew without bound.

The fix is the flow-control rule in §4 (only progress sends more). `catchUpTrafficStaysBoundedOnADuplicatingNetwork` is the regression test: it **fails on the old code** and passes on the new one, keeping a 400-entry catch-up under 5,000 messages. The two seeds now finish in about 0.6 s.

Fixing it exposed a second, harmless effect. The runaway chains had accidentally sped up catch-up. Without them, a follower hundreds of entries behind with one-entry batches catches up one entry per round trip, which can take longer than 5 seconds. The final liveness check now allows 60 s of virtual time (the property is "eventually"). Real batches are 64 entries, and pipelining in Milestone 11 speeds this up properly.

## 9. Check yourself

1. A follower's log is `[1@1, 2@1, 3@2]` and it receives `AppendEntries(prev=2@1, entries=[3@3, 4@3])`. What does its log look like afterwards, and which rule did what?
2. Why must a follower never truncate when the incoming entries already match? Describe the message reordering that makes it dangerous.
3. In Figure 8, y2 was on a majority. Explain exactly how it could still be lost, and which line of `advanceCommitIndex` prevents committing it.
4. Why does the leader append a no-op when elected? What would a client observe without it?
5. A leader gets a rejection with `conflictTerm=2, conflictIndex=11`. Its own log has term-2 entries at 15-18. What is the new `nextIndex`, and why not 11?
6. A proposal's future fails with `NotLeaderException`. Can the command still end up applied? What does the client need to do about that?
7. How does the invariant checker verify Log Matching without comparing every pair of logs on every event?
8. Why did the polite chaos client miss the Figure 8 bug, and what does that teach about designing randomized tests?
9. Explain how one duplicated AppendEntriesReply could create a message chain that never ends, and why "only progress sends more" stops it.
