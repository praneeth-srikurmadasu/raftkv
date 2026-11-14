# Milestone 5: The KV store, exactly-once sessions, and the client explained

Raft gives us a replicated log. This milestone builds the thing people actually use on top of it: a key-value store (`PUT`, `GET`, `DELETE`, `CAS`), a client library that finds the leader and retries, and **sessions** that make those retries safe.

```
protocol/
├── Message.java          sealed: RaftMessage | ClientRequest | ClientReply
├── RaftMessage.java      the 7 Raft messages (they carry a term)
├── ClientRequest/Reply   (clientId, sequence, ackedBelow, command) / (sequence, status, leaderHint, result)
└── ClientStatus.java     OK, NOT_LEADER, SESSION_EXPIRED, INVALID_REQUEST, (WRONG_GROUP, SHARD_NOT_READY, OVERLOADED)
kv/
├── KVCommand / KVResult  Put, Get, Delete, Cas and their results
├── KVCodec               binary encodings (commands, results, log payloads, outcomes)
├── SessionCommand        what goes into a log entry: client, sequence, watermark, leader timestamp, command
├── SessionTable          per-client cached results + ack watermark
├── KVStateMachine        sorted map + sessions; deterministic; digest() for replica comparison
└── KVServer              ClientRequest → propose → ClientReply
client/
├── RaftKVClient          leader discovery, retries with backoff + jitter, async + blocking API
├── ClientConfig          timeouts and backoff
└── SessionExpiredException
```

---

## 1. Why exactly-once needs sessions

A client sends `PUT x=5`. The leader appends it, replicates it, commits it, applies it, and then the reply is lost (or the leader crashes before replying). The client times out and retries. Without extra work, the PUT is now in the log **twice**. For `PUT` that may look harmless, but:
- `PUT` returns the previous value: the retry returns `5`, its own write.
- A client doing "read, then CAS", or anything that counts, gets the wrong answer.
- Another client's write between the two copies gets silently overwritten by the stale retry.

Raft §8: *"each command gets a unique serial number; the state machine tracks the latest serial number processed for each client, along with the associated response. If it receives a command whose serial number has already been executed, it responds immediately without re-executing the request."*

## 2. Our design: cached results + an acknowledgement watermark

Every request carries `(clientId, sequence, ackedBelow)`:
- `clientId`: a random 128-bit id, one per client session.
- `sequence`: 1, 2, 3, ... per client. **A retry reuses the same number.**
- `ackedBelow`: the lowest sequence the client has *not* yet received a reply for.

The state machine keeps, per client, the results of requests the client hasn't acknowledged yet. On apply:

| Situation | Action |
|---|---|
| unknown client, `ackedBelow == 1` | new session (nothing could have been forgotten yet) |
| unknown client, `ackedBelow > 1` | we had a session and expired it, so we can't tell if this ran → `SESSION_EXPIRED`, don't execute |
| `sequence < ackedBelow` | the client already has this reply (a stale duplicate) → don't execute |
| result cached for `sequence` | **a retry → return the cached result, don't execute** |
| otherwise | execute, cache, return |

Results below `ackedBelow` are deleted, so memory stays bounded by what's actually in flight.

**Why not just "last sequence per client"?** That only works if a client has one request in flight at a time. Our client is asynchronous: request 2 can commit before request 1. With "last sequence = 2", the late request 1 would look like an old duplicate and never run. Caching per sequence plus a watermark (the thesis §6.3 / RIFL approach) handles any number of in-flight requests. `outOfOrderRequestsFromOneClientEachExecuteOnce` tests exactly that.

## 3. Determinism: the rule everything rests on

Every replica applies the same entries, and must end up **byte-for-byte identical**. So the state machine may not use clocks, randomness or `HashMap` order. Two consequences:

- **Session expiry uses log time.** The leader stamps each entry with its wall clock. The state machine keeps `logTime = max(logTime, entry.timestamp)`, so time never runs backwards even if a new leader's clock is behind, and it expires sessions idle for longer than the timeout in *that* time. Every replica expires the same session at the same log index. With local clocks, one replica could expire a session that another still has, and they would answer the same retry differently.
- **Bad input never throws.** If `apply` threw on a malformed command, *every* replica would crash at the same entry, and keep crashing on replay. Garbage becomes `INVALID_REQUEST`.

`replicasThatApplyTheSameEntriesAreIdentical` feeds 50 random histories (retries, jittery and backwards clocks) to two state machines and compares their `digest()`.

## 4. The server (`KVServer`)

```
ClientRequest ──▶ not leader?            → NOT_LEADER (+ hint)
              ──▶ command malformed?     → INVALID_REQUEST (no log entry)
              ──▶ propose(SessionCommand{client, seq, ackedBelow, leader wall clock, command})
                    committed + applied  → reply with the state machine's outcome
                    lost leadership      → NOT_LEADER (the entry may STILL commit later!)
```
That last line is why sessions are mandatory: "NOT_LEADER" doesn't mean "didn't happen".

## 5. The client (`RaftKVClient`)

- **Leader discovery:** start anywhere. A `NOT_LEADER` reply carries a hint, and the client remembers the leader. `clientThatOnlyKnowsAFollowerFindsTheLeader`.
- **Retries:** if a reply doesn't arrive in time, forget the leader guess and retry elsewhere after a backoff: random in `[0, min(max, base·2^attempt)]` (exponential backoff with **full jitter**). Without jitter, every client that failed at the same moment retries at the same moment, and the new leader drowns in a synchronized wave.
- **Deadline:** after `deadline` (15 s by default) the operation fails with `TimeoutException`. The outcome is then *unknown*: it may or may not have happened.
- **Threading:** all client state lives on its event loop. Public async methods just schedule work. The blocking `put/get/...` helpers must never be called *on* that loop (the loop would wait on itself).

## 6. Testing

| Test | Proves |
|---|---|
| `KVCodecTest` (4) | 1,000 random round trips; every truncation rejected; size limits; unknown tags |
| `KVStateMachineTest` (9) | map semantics, retry = cached answer, out-of-order requests, watermark, expired sessions, log-time expiry with backwards clocks, garbage → INVALID, replica determinism, the dedup bug |
| `KVClusterTest` (9) | end-to-end ops, leader discovery, leader crash mid-stream, **exactly-once** (below), 200 concurrent ops, timeout without a majority, session expiry surfaced to the app, oversized keys, full restart on disk |
| `KVChaosTest` | **gate:** 300 chaos runs (half on simulated disks) with 3 clients writing through the real client library |

**How exactly-once is checked from the outside.** One client writes `v1, v2, ... v40` to one key, one at a time, over a network that drops and duplicates 20% of messages, with 40 ms client timeouts (it retries constantly) and two leader crashes. Each `PUT` returns the previous value, so it must return `v(i-1)`. A retry that executed twice would return its own value; a lost acknowledged write would return something older. At the end, every replica must have executed exactly 40 commands. The chaos test applies the same chain check to three concurrent writers and also compares every replica's `digest()`.

**Bug detection:** `SESSION_DEDUP_DISABLED` is caught at seed 1.

### A test bug worth knowing about
The first chaos run failed one seed in 300: "n5 has a different KV state than the leader". The cause wasn't the KV store. The check waited until every node had applied *up to the leader's commit index*, but a late duplicate entry then committed. When the digests were compared, the leader was at 264 and n5 at 263. **Replica states can only be compared at the same log position.** The helper `awaitIdenticalApplied` now waits until all nodes have applied exactly the same prefix.

## 7. Check yourself

1. A PUT commits but its reply is lost; the client retries. Walk through what the state machine does with the retry, field by field.
2. Why can't "last sequence number per client" handle an asynchronous client? Give a concrete interleaving.
3. Why must session expiry use timestamps from the log rather than each replica's clock?
4. A client gets `NOT_LEADER` after the leader accepted its request. Can the request still take effect? What makes the retry safe?
5. What does `SESSION_EXPIRED` tell the application, and what should it do next?
6. Why does the client use *full jitter* in its backoff instead of a fixed exponential delay?
7. Why does a malformed command become `INVALID_REQUEST` inside `apply()` instead of throwing an exception?
8. Why did comparing replica digests at "applied ≥ leader's commit index" give a false alarm?
