# Milestone 2: Leader election explained

Raft splits consensus into three parts: **leader election**, **log replication**, and **safety**. This milestone builds the first one: at any moment, a group of servers agrees on at most one leader per term, and elects a new one quickly when the old one fails.

New code:

```
raft/
├── RaftNode.java        the election state machine (one replica of one group)
├── RaftConfig.java      timeouts + injected bugs (tests only)
├── Role.java            FOLLOWER, CANDIDATE, LEADER
├── Bug.java             deliberate bugs for the bug-detection suite
├── log/RaftLog.java     log interface (index 0 = virtual empty entry)
├── log/InMemoryLog.java
└── storage/             PersistentState, MetaStore, InMemoryMetaStore
```

---

## 1. Terms: Raft's logical clock

Time is divided into **terms**, numbered 1, 2, 3, ... Each term starts with an election; if someone wins, they lead for the rest of the term. Every message carries the sender's term, and one rule keeps everybody in sync:

> **Fig.2 All Servers:** if a message contains term T > currentTerm, set currentTerm = T and become a follower.

That single rule is how a stale leader finds out it has been replaced: the first reply with a newer term makes it step down. In `RaftNode.handle()` it runs before any message-specific handling.

## 2. The three roles

```
           election timeout                majority of votes
 FOLLOWER ─────────────────▶ CANDIDATE ──────────────────▶ LEADER
    ▲  ▲                        │  │                          │
    │  └── AppendEntries from ──┘  └─ timeout: new election   │
    │      leader of this term       (term + 1)               │
    └────────────── sees a higher term ───────────────────────┘
```

| Role | Does | Code |
|---|---|---|
| Follower | answers votes and heartbeats; election timer runs | `onRequestVote`, `onAppendEntries` |
| Candidate | term+1, votes for itself, persists, asks everyone | `startElection`, `sendVoteRequests` |
| Leader | sends heartbeats every 50 ms so nobody times out | `becomeLeader`, `sendHeartbeats` |

## 3. An election, step by step (3 nodes)

```
t=0      n1, n2, n3 start as followers in term 0. Each picks a random timeout in [150, 300] ms.
t=183    n2's timer fires first.
           n2: currentTerm=1, votedFor=n2, SAVE to MetaStore, then RequestVote(t1) → n1, n3
t=186    n1 receives it: term 1 > 0 → adopt t1. Not voted yet, n2's log is up-to-date
           → votedFor=n2, SAVE, then reply granted. Reset own election timer.
t=189    n2 receives n1's vote: n2 + n1 = 2 of 3 = majority → LEADER
           → immediately sends heartbeats (empty AppendEntries) to n1 and n3, then every 50 ms
t=191    n3 gets n2's heartbeat for t1 → follower of n2 (n3's vote no longer matters)
```

## 4. Why each rule exists

**Randomized timeouts (§5.2).** If every follower waited exactly 200 ms, they would all become candidates at once, each vote for itself, and nobody would win. This is a *split vote*, and it could repeat forever. Random timeouts mean one node usually fires first and wins before the others wake up. `splitVotesResolveQuickly` checks 200 seeds of 4- and 6-node clusters, where splits are most likely.

**One vote per term (§5.2).** Two majorities of the same cluster always overlap in at least one server. If every server votes at most once per term, two candidates can't both collect a majority, so there is at most one leader per term (**Election Safety**).

**Persist before replying (Fig. 2).** Suppose n1 votes for n2 in term 5, crashes, restarts and has forgotten. It could vote for n3 in term 5 too, and now n2 and n3 might both be leaders of term 5. So `votedFor` is saved before the reply leaves. `higherTermResetsTheVoteAndIsPersistedBeforeReplying` captures the store's contents *at the moment the reply is sent*.

**The "up-to-date" check (§5.4.1).** A voter refuses a candidate whose log is behind its own: an older last term, or the same last term but shorter. A committed entry is on a majority, so any winner must have gotten a vote from at least one server holding it. That server refuses candidates that lack the entry, so **every new leader already has every committed entry** (Leader Completeness). This matters from Milestone 3 on; we test it now with pre-filled logs (`nodeWithStaleLogNeverWins`).

**Vote request retries (§5.1).** "Servers retry RPCs if they do not receive a response in a timely manner." A candidate re-sends RequestVote every heartbeat interval to peers that haven't answered. Without this, one lost message wastes a whole election timeout on a lossy network.

**Leader stickiness (thesis §4.2.3).** A node that heard from a leader within the minimum election timeout (150 ms) **ignores** RequestVotes. It doesn't grant them, and it doesn't adopt their term. This stops a node that was briefly disconnected and inflated its term from forcing a pointless election. (`leadershipTransfer=true` bypasses it, for Milestone 12.)

## 5. Testing it

### Rules in isolation: `VoteRulesTest`
One node, a transport that records what it sends, and a store we can inspect. 16 tests, one per rule: stale terms, double votes, the log comparison, stickiness edges (149 ms vs 150 ms), restart memory, vote retries, late replies from old elections.

### Whole clusters: `ElectionTest`
Single node, 3 and 5 nodes, crashed leaders, isolated leaders, minority partitions, rejoining nodes, stale logs, split votes, and same-seed determinism.

### Invariants after every event: `ElectionInvariants`
Registered with `sim.addInvariant(...)`, so they run after **every** simulated event, millions of times across the suite. The checker keeps its own history, so it notices a violation even if the culprit crashed afterwards:
- **Election Safety:** never two leaders in one term.
- **One vote per term:** no node changes its vote within a term (across restarts too).
- **Monotonic terms:** no node's term ever goes backwards.

### Chaos: `ElectionChaosTest` (the milestone gate)
500 runs, each with a seed-chosen mix (swarm testing): 3 or 5 nodes, latency up to 30 ms, up to 15% drops, up to 10% duplicates, and a nemesis that every 100-600 ms partitions, isolates, heals, crashes (often the leader) or restarts nodes for 10 s. Then everything heals and the group must converge on one leader and keep it for 5 s. It takes about 3 seconds. I also ran **30,000 random seeds** before shipping: 0 failures.

### Proof the tests have teeth: `ElectionBugDetectionTest`
| Injected bug | Caught by | Seeds to detection |
|---|---|---|
| `VOTE_WITHOUT_LOG_CHECK` | stale-log node wins | 1 |
| `LEADER_IGNORES_HIGHER_TERM` | group never converges after the heal | 3 |
| `VOTE_TWICE_IN_TERM` | one-vote-per-term invariant | 4 |
| `VOTEDFOR_NOT_PERSISTED` | one-vote-per-term after a crash/restart | 33 |

## 6. A real finding: disruptive rejoins

In the 20,000-seed stress run, 0.2% of runs failed the stability check. None of them broke safety. Tracing one seed (`sim.echoTraceTo(System.out::println)`) showed:

```
9.840s net partition {n1=1}            n1 is cut off for 215 ms and misses heartbeats
10.055s net heal
10.069s [n1 t12 CANDIDATE] starting election          its timer fires just after the heal
10.071s [n2 t11 LEADER] rejected vote for n1: current leader is alive   (stickiness works)
10.084s [n2 t12 FOLLOWER] stepped down: saw higher term from n1         (but this rule still applies)
10.127s [n1 t12 LEADER] became leader
```

n1 rejected n2's heartbeat because its term (12) was higher, and the reply carried t12. Fig. 2 says any higher term makes the leader step down. Stickiness protects the followers' votes, but nothing stops the term from spreading. The cure is **pre-vote** (Milestone 11): n1 first asks "would you vote for me?" *without* raising its term, gets told no, and never disrupts anyone. For now the chaos test lets the group settle 2 s after the heal before measuring stability.

## 7. Replaying a seed

```powershell
.\mvnw.cmd test "-Dtest=ElectionChaosTest" "-Draftkv.seed=7820414455560660922"
.\mvnw.cmd test "-Dtest=ElectionChaosTest" "-Draftkv.seeds=5000"     # more random seeds
```
To see every event of a run, call `c.sim.echoTraceTo(System.out::println)` in a test.

## 8. Check yourself

1. Two candidates in a 5-node cluster each get 2 votes in term 7. What happens next, and why does it eventually resolve?
2. Why must a voter persist `votedFor` *before* sending its reply? Sketch the two-leaders scenario if it didn't.
3. Node A's log ends `[..., 7@t3]`, node B's ends `[..., 5@t4]`. Who is more up-to-date? Why is "longer log wins" wrong?
4. A leader is cut off from everyone for 10 s and then reconnects. What does it believe during the partition? What happens in the first 100 ms after it reconnects?
5. Why does stickiness check "heard from a leader within the *minimum* election timeout", not the maximum?
6. A RequestVoteReply for term 6 arrives when the candidate is already in term 7. Why must it be ignored?
7. In the disruptive-rejoin trace, stickiness worked but the leader still stepped down. Which exact rule caused that, and how does pre-vote avoid it?
8. Why is "one vote per term" a better thing to check than "at most one leader per term"?
