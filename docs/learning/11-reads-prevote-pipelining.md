# Milestone 11: Faster reads, calmer elections, pipelined replication, explained

Until now every GET went through the log like a write: one entry, one fsync on a majority, just to read a value. Elections could be disturbed by a node that had been cut off, and the leader sent one AppendEntries at a time to each follower. This milestone fixes all three, and proves the new code with the same chaos suites, scripted scenarios, and injected bugs as before.

```
src/main/java/dev/raftkv/raft/
├── ReadMode.java          LOG, READ_INDEX, LEASE
├── RaftConfig.java        + preVote, checkQuorum, readMode, maxInflightAppends, maxClockDrift
├── Role.java              + PRE_CANDIDATE (and LEARNER, Milestone 12)
├── RaftNode.java          read(), pre-vote, check-quorum, heartbeat rounds, pipelining
└── LeaderState.java       + per-follower window, last ack, echoed round
src/main/java/dev/raftkv/protocol/AppendEntries(.Reply).java   + round (echoed)
src/test/java/dev/raftkv/raft/
├── ReadsTest.java              ReadIndex, leases, batching, a cut-off leader, the skewed-clock test
├── ReadBugDetectionTest.java   scripted windows for the two read bugs
└── ElectionTest / ElectionBugDetectionTest   pre-vote, check-quorum, the timer-reset bug
```

---

## 1. Three ways to read (SPEC §4.4, thesis §6.4)

**LOG.** The GET becomes a log entry. Always correct, and as slow as a write.

**READ_INDEX.** The leader may answer from its own state machine, if it can prove two things:
1. *It knows everything committed.* It writes down `readIndex = commitIndex`, but only once an entry of its own term has committed (its no-op, appended right after the election). Before that, its commit index may be behind what the previous leader committed.
2. *It is still the leader.* A newer leader could exist without our knowing. So the leader sends a heartbeat round and waits until a majority answers. Nobody can have been elected without one of those voters noticing.

Then it waits until it has applied up to `readIndex`, and reads. No log entry, no fsync, one round trip.

How does the leader know which reply belongs to which round? Every AppendEntries now carries a **round number**, and every reply echoes it. A read that arrived during round 41 is confirmed once a majority has echoed round 42 or later. Reads arriving close together share the same round (**batching**): 100 concurrent reads cost one heartbeat to each follower (`manyConcurrentReadIndexReadsShareOneHeartbeatRound`).

**LEASE.** If a majority confirmed a round sent at time `t`, then for one election timeout after `t` no other leader can be elected: every follower that answered refuses to vote while it still hears from the leader (**leader stickiness**, already there since Milestone 2). So until `t + electionTimeoutMin / (1 + maxClockDrift)` the leader answers reads at once, with no round trip at all. When the lease runs out, a read falls back to READ_INDEX.

The catch: a lease measures time on the leader's clock. If that clock runs slow, the leader thinks its lease lasts longer than it really does. `maxClockDrift` (default 10%) shortens the lease to allow for that. `aClockSkewedBeyondTheBoundBreaksLeasesAndTheCheckerNoticesIt` makes the leader's clock run at 1/10 of real speed, cuts the leader off, lets the others elect a new leader and write, then reads from the old one: the stale answer comes back and the linearizability checker rejects the history. With an accurate clock the same scenario serves no read at all.

| Mode | Cost per read | Assumption |
|---|---|---|
| LOG | 1 log entry, fsync on a majority | none |
| READ_INDEX | 1 heartbeat round (shared) | none |
| LEASE | nothing while the lease holds | clocks drift less than `maxClockDrift` |

GETs on the sharded store also check, after the read is confirmed, that the shard still belongs to the group (it may have moved meanwhile).

One subtlety in the client: a read answered without the log creates no session on the server. Until a write has succeeded, the client keeps telling servers "I have seen no replies" (`ackedBelow = 1`), so a server never mistakes a new client for an expired one.

## 2. Pre-vote (thesis §9.6)

Before this milestone, a node cut off from the others timed out again and again, raising its term each time. When it came back, its high term forced a healthy leader to step down. Now, on timeout a node first becomes **PRE_CANDIDATE** and asks "would you vote for me in term T+1?" *without* changing its term. A voter says yes only if the candidate's log is up to date and it hasn't heard from a leader recently. Only with a majority of yeses does the node start a real election.

`withPreVoteARejoiningNodeChangesNothing`: a node isolated for 5 seconds runs many pre-votes, but its term never moves and the leader is never disturbed.

## 3. Check-quorum

A leader that hasn't heard from a majority within an election timeout steps down on its own. Without this, a leader on the losing side of a partition keeps believing it leads until the partition heals. Leases need it, and it makes failures visible sooner.

## 4. Pipelining

The leader used to have at most one AppendEntries in flight per follower, sending more only when a reply showed progress. Now up to `maxInflightAppends` (default 8) batches can be in flight, each continuing where the previous one ended. Getting this right took three rules:
- **Progress only.** A reply opens the window only if it moved the follower's match index forward. Duplicated or stale replies send nothing (this is what fixed message amplification in Milestone 3).
- **Go back after silence.** If a heartbeat interval passes with batches in flight but no progress, the leader goes back to the confirmed position and resends (go-back-N). Lost messages cost at most one heartbeat.
- **Go back once on a gap.** When batches overtake each other, the follower rejects the later one ("my log is too short"). The leader resends from the gap, but only once per progress. Otherwise every out-of-order rejection would trigger a burst, and each burst more rejections.

Two bugs were found on the way, both by the chaos suites: a follower stuck after receiving a snapshot (its window still counted batches the snapshot had made irrelevant), and catch-up traffic growing much faster than the work. Both are covered by tests now.

**Does pipelining make things faster?** The first benchmark said no: on loopback with real fsyncs it was *slower* than stop-and-wait (10,000 vs 17,000 writes/s). The reason: every AppendEntries cost the follower its own fsync, and stop-and-wait happens to collect more entries per message. The fix is **follower group commit**: a follower appends, schedules one flush, and sends the success replies for every message that arrived meanwhile after that single fsync. (A reply that promises durability still never leaves before the fsync; an ack from an older term, or for entries truncated meanwhile, is dropped and the leader retransmits.) After the fix, writes doubled across the board and pipelining wins: 19,800 vs 16,100 writes/s, with a lower p99. See `docs/benchmarks.md`.

## 5. Bugs that must be caught

| Injected bug | How it is caught |
|---|---|
| `READINDEX_WITHOUT_HEARTBEAT` | scripted: a cut-off leader (check-quorum off) answers a read after the new leader's write; the checker rejects the history |
| `LEASE_WITHOUT_STICKINESS` | scripted: a node the leader can't reach gets votes from a follower that still hears the leader, while the lease holds; stale read, rejected |
| `RESET_TIMER_ON_REJECTED_VOTE` | scripted liveness: only one of 4 surviving nodes is electable; with the bug, the other 3 keep resetting its timer and no leader appears within a second |

Why scripted, not chaos? Check-quorum and stickiness close almost the whole window in which these bugs do harm, so random faults rarely hit it (300 chaos seeds found neither read bug). The scripted tests drive the system straight into that window, then use the same checker.

Also: pre-vote hides `LEADER_IGNORES_HIGHER_TERM` and `VOTEDFOR_NOT_PERSISTED` almost completely (terms rarely race ahead, real elections become rare), so those two classic-path bugs are now tested with pre-vote switched off. `VOTE_WITHOUT_LOG_CHECK` is applied to the pre-vote check too, as a real implementation would share that code.

## 6. Every chaos suite now also randomizes these

Per seed: read mode (a third each), window 1/2/8, pre-vote on in 3 of 4 runs, check-quorum on in 3 of 4 (always with leases). Everything from Milestones 2-10 still passes, now in every combination.
