# Milestone 6: Proving the store is linearizable, explained

Milestones 2 to 5 checked Raft's *internal* rules (one leader per term, logs match, committed entries never change). Users don't see those rules. They see answers. This milestone checks the answers: under crashes, partitions, lost and duplicated messages, does every client get results that a single, correct, non-replicated map could have given?

```
src/test/java/dev/raftkv/
├── linearizability/
│   ├── Operation.java                 one client call: invoke time, return time (or unknown), answer
│   ├── History.java                   records operations as they happen
│   ├── LinearizabilityChecker.java    the search (Wing & Gong + Lowe's memoization = Porcupine)
│   └── LinearizabilityCheckerTest     14 hand-built good and bad histories
└── raft/
    ├── LinearizabilityChaos.java      swarm faults + concurrent clients + recording
    ├── LinearizabilityChaosTest       the gate: 1,000 runs (MEMORY and DISK storage alternate)
    └── LinearizabilityBugDetectionTest   injected bugs must be caught
```
Main code changes are small: `SimulatedNetwork.blockOneWay(from, to)` (asymmetric link failures) and a deliberately broken read path, `Bug.READ_FROM_LOCAL_STATE`, used only by the bug-detection test.

---

## 1. What "linearizable" means

Every operation has two instants: when the client **invoked** it and when the answer **returned**. Between those, the system did *something*. Linearizability says: you can pick one instant inside each operation's interval where it "really happened", such that

1. replaying the operations one at a time in the order of those instants gives exactly the answers the clients got, and
2. an operation that returned before another was invoked comes first.

```
client A:  |--- PUT x=1 ---|
client B:        |------ GET x → 1 ------|        ✓  (B's GET can happen after A's PUT)
client C:                                 |-- GET x → (absent) --|   ✗  (x=1 was already visible to B,
                                                                       and C started after B finished)
```
Rule 2 is what makes it stronger than "serializable": real-time order between clients matters. If your friend reads a value after you were told your write succeeded, they see it.

## 2. Why the result of a check is either "yes" or "here is the proof it is impossible"

The checker searches for that order. If one exists, the history is linearizable. If **no** order exists, it reports the smallest stuck point. There's no "probably": the checker either finds a witness or exhausts every possibility.

Searching all orders is exponential, so two classic tricks keep it fast:

- **Locality** (Herlihy & Wing): a history is linearizable if and only if each object's sub-history is. Every KV operation touches one key, so each key is checked separately. A run with 400 operations on 4 keys becomes 4 searches of ~100.
- **Memoization** (Lowe): the search state is (which operations are already placed, the map's current value). Reaching the same state twice by a different path can't lead anywhere new, so it is cached. This is what turns an exponential search into milliseconds in practice.

The search itself (Wing & Gong) walks invocation and return events in time order:
- at an invocation, try "it happens now": if the model gives the same answer the client got, place it and restart from the front;
- at a return of an operation that hasn't been placed, the current guess is impossible (it had to happen before it returned): undo the last choice and try the next one.

Measured on the gate: p99 check time 3 ms, worst 10 ms.

## 3. Unknown outcomes: the hard part

A client times out. Did its `PUT` happen? Maybe: the leader may have committed it just before the client gave up. Maybe not. A checker that treats timeouts as failures gives false alarms; one that ignores them misses real bugs.

The standard answer (Jepsen, Porcupine): give the operation a **return time of infinity** and accept any answer. The search may place it anywhere after its invocation, including "at the very end", which for everything observable means "never happened". An unknown `GET` can't affect anyone, so it is dropped entirely.

The catch: every unknown operation stays "open" forever, so many of them make the search explode. An early version of the chaos run produced 219 unknown operations out of 275 in one run and the checker gave up. Two fixes, both copied from how Jepsen works:
- a client whose operation timed out is **retired** (it may still have a request in flight) and replaced by a fresh client with a new session, at most 3 times per run;
- "the checker ran out of budget" is reported as **inconclusive**, which the gate counts as a failure. It never counts as "linearizable", and the bug-detection test does not count it as "bug caught" either. Only a proven violation counts.

Result: 696 unknown outcomes among 393,611 operations, zero inconclusive checks.

## 4. The chaos run

Each seed builds a 3- or 5-node cluster and turns fault types on or off independently (**swarm testing**: some runs are all partitions, some all crashes, some everything at once):

| Fault | How |
|---|---|
| Partitions | random split, healed later |
| One-way link failures | `blockOneWay(a, b)`: a's messages to b vanish, b's to a arrive |
| Crashes and restarts | volatile state lost; with DISK storage, unsynced writes lost or torn |
| Drops, duplicates | per-message probability |
| Latency spikes | delays that reorder messages |

Meanwhile 3 to 5 clients run a random mix on 2 to 4 shared keys: 35% PUT, 35% GET, 20% CAS, 10% DELETE. Every written value is unique, so a stale read can't accidentally look right.

```powershell
.\mvnw.cmd test "-Dtest=LinearizabilityChaosTest"                       # the 1,000-run gate
.\mvnw.cmd test "-Dtest=LinearizabilityChaosTest" "-Draftkv.seeds=5000"  # more
.\mvnw.cmd test "-Dtest=LinearizabilityChaosTest" "-Draftkv.seed=42"     # replay one run
```

## 5. Does the checker actually catch bugs?

A checker that always says "yes" passes every test. So we break the system on purpose and require a proven violation:

| Injected bug | What it does | Caught at |
|---|---|---|
| `SESSION_DEDUP_DISABLED` | a retried command executes twice | seed 1 |
| `READ_FROM_LOCAL_STATE` | any replica answers GET from its own, possibly stale, state | seed 7 |

The second one is the classic mistake ("followers are up to date, mostly"). It passes every single-client test. Only concurrent clients plus a partition expose it: a client reads an old value from a cut-off follower after another client's newer write was already acknowledged.

`LinearizabilityCheckerTest` also feeds 14 hand-built histories (stale reads, lost writes, CAS against the wrong value, real-time order violations, unknown outcomes that do and don't explain a result) and checks each verdict.

## 6. A bug in a test, found by more seeds

The chaos tests pick fresh random seeds on every run, so each `mvnw test` explores new executions. While running the full suite for this milestone, a replication invariant fired:

```
Leader Completeness violated: new leader n5 of t5 lacks committed entry 3@t6
```
The entry's own term (6) is *higher* than the leader's term (5), so no leader of term 5 could ever have it. Raft wasn't wrong; the **check** was. Leader Completeness says an entry committed in term T must be in the logs of leaders of terms *after* T. The check compared against every committed entry. What happened: n5 collected votes for term 5, the vote replies were delayed, a term-6 leader committed entries meanwhile, and only then did the replies reach n5 and make it (briefly) leader of term 5. That is legal; n5 steps down the moment it hears from term 6.

Fix: remember the term each index was committed *in* (the term of the leader that first committed it) and only require entries committed in earlier terms. Lesson: invariant checkers need the same precision as the code they check, and random seeds find the gaps in both.

## 7. What's next

Milestone 7 moves everything off the simulator: real TCP between real processes, a real disk, a command-line client, and a test that kills the leader *process* and checks nothing acknowledged was lost.
