# Milestone 1: Foundations explained

Before any Raft logic, we build the four things every later milestone stands on:

1. **Injectable time and scheduling**, so tests control the clock.
2. **A binary wire format**, so nodes can talk without frameworks.
3. **Sealed message types**, so the compiler catches a forgotten message.
4. **A deterministic simulator and network**, so we can test failures that are almost impossible to reproduce on real machines.

```
 dev.raftkv
 ├── common      Bytes, NodeId, GroupId, Clock, Scheduler, Hashing, EventLoopScheduler
 ├── codec       BinaryWriter/Reader, Frame, FrameCodec, FrameDecoder   (bytes ⇄ frames)
 ├── protocol    sealed Message + 7 Raft messages + MessageCodec        (frames ⇄ messages)
 ├── raft/log    LogEntry, EntryType
 ├── sim         Simulation, Seeds                                      (virtual time + event queue)
 └── transport   Transport, Envelope, MessageHandler
     └── sim     SimulatedNetwork                                       (faulty in-memory network)
```

---

## 1. Determinism: why time and randomness are injected

Distributed-systems bugs depend on **timing**: a message arrives 1 ms after a timeout fires instead of 1 ms before. On real machines you can't make that happen on purpose, and once it does happen you can't make it happen again.

So the rule (SPEC §3.2) is that Raft code never reads the real clock, never creates its own `Random`, and never starts threads. It receives:

| Interface | Production | Tests |
|---|---|---|
| `Clock` | `SystemClock` (`System.nanoTime`) | `ManualClock`, moved only by the simulation |
| `Scheduler` | `EventLoopScheduler` (one real thread) | `Simulation` (virtual time) |
| `Random` | seeded from anything | seeded from the test's seed |
| `Transport` | NIO TCP (Milestone 7) | `SimulatedNetwork` |

Same code, two worlds. In the simulated world, **seed + code ⇒ exactly one possible execution**.

### The event loop (`Scheduler`)
Each Raft node is a single-threaded event loop: every event (timer fired, message arrived, disk write finished) is a task on one thread. Benefits: no locks in the Raft core, no data races, and a clear order of events. The cost is one CPU core per Raft group, which multi-Raft (Milestone 9) wins back by running many groups per process.

`EventLoopScheduler` wraps a one-thread `ScheduledThreadPoolExecutor`, with one important fix: a plain executor **silently swallows** exceptions thrown by scheduled tasks (they're stored in a `Future` nobody reads). Ours passes them to an error handler, so a bug can't fail silently.

---

## 2. `Bytes`: why not `byte[]`?

```java
record LogEntry(long index, long term, byte[] data) {}
new LogEntry(1, 1, new byte[]{7}).equals(new LogEntry(1, 1, new byte[]{7}))  // false!
```
Records generate `equals` from their fields, but arrays compare by **identity**. Every round-trip test would quietly compare references. `Bytes` is an immutable wrapper with content equality, content hashing, unsigned lexicographic ordering (the order KV keys sort in), and readable `toString`. It copies on the way in and out, so a log entry can never be changed after creation.

---

## 3. The wire format

### `BinaryWriter` / `BinaryReader`
Big-endian (network byte order), fixed-width integers, `[length:4][bytes]` for variable data. Hand-written instead of Java serialization because:
- **Security:** deserializing untrusted bytes with `ObjectInputStream` has caused many remote-code-execution vulnerabilities.
- **Stability:** Java serialization ties the format to class internals; renaming a field breaks old nodes.
- **Size and speed:** no class names or metadata on the wire.

The reader treats every input as **hostile**:
- Every read checks the remaining bytes and throws `CodecException`, never `BufferUnderflowException`.
- A length prefix is checked against the bytes actually present **before** allocating. Otherwise 4 corrupt bytes saying "2 GB follows" would cause an out-of-memory crash.
- Booleans must be exactly 0 or 1, and strings must be valid UTF-8 (rejected, not "repaired").
- `expectEnd()`: leftover bytes after a message mean sender and receiver disagree about the format, which is an error.

### Frames (`FrameCodec`, `FrameDecoder`)
```
[length:4][type:2][groupId:4][correlationId:8][flags:1][payload:N][crc32c:4]
```
- **length**: TCP is a byte stream, not a message stream. One `read()` can return half a frame or three frames. The length prefix tells the decoder where each frame ends.
- **groupId**: lets one TCP connection carry every Raft group two processes share (multi-Raft, Milestone 9).
- **correlationId**: matches responses to requests for client RPCs (Milestones 5 and 7). Raft itself doesn't need it.
- **crc32c**: detects corruption. TCP's own checksum is only 16 bits and misses real-world errors. The CRC also covers the length field. The same idea will protect our on-disk log records.

`FrameDecoder` is fed arbitrary chunks and returns whole frames. After any `CodecException` it refuses more input, because once the stream is out of sync there's no way to find the next frame boundary, so the connection must be closed.

---

## 4. Sealed messages

```java
public sealed interface Message permits RequestVote, RequestVoteReply, AppendEntries, ... {}
```
Because the compiler knows the complete list, a `switch` over `Message` without `default` is checked for **exhaustiveness**. `MessageCodec.encode` is such a switch, and `decode` switches over the `MessageType` enum. Add an eighth message and the codec stops compiling until you handle it. A whole class of "forgot to serialize the new field/type" bugs becomes a compile error.

### Messages, not RPC calls
Raft is described in terms of RPCs, but we implement it as **one-way messages**: a reply is just another message sent back. This is the etcd/raft style. Raft is designed to tolerate lost, duplicated and reordered messages anyway, so the core never needs to wait on a future. Each reply carries enough information (term, `matchIndex`, `preVote` echo) to be handled correctly even if it arrives late or twice.

### What the 7 messages are for
| Message | Raft section | Purpose |
|---|---|---|
| `RequestVote` / `Reply` | Fig. 2, §5.2; pre-vote thesis §9.6 | elections |
| `AppendEntries` / `Reply` | Fig. 2, §5.3 | replication and heartbeats; reply carries fast-backtracking hints |
| `InstallSnapshot` / `Reply` | Fig. 13, §7 | catch up a follower that's too far behind |
| `TimeoutNow` | thesis §3.10 | leadership transfer |

Records validate their own invariants (no negative terms; `AppendEntries` entries must directly follow `prevLogIndex`). A decoded message that breaks one becomes a `CodecException`, so bad input can never become a "valid" message.

---

## 5. The simulator

`Simulation` keeps a priority queue of `(time, tieBreaker, sequence, task)`:

```
 step():  take the earliest event → move the virtual clock to its time → run it
```
- **No real waiting.** The clock jumps from event to event; 10 simulated seconds of a busy cluster take about 5 ms.
- **Ties are broken by the seed.** Events at the same instant run in a seed-chosen order. One seed always gives the same order; different seeds explore different interleavings.
- **Trace hash.** Components call `sim.trace("net deliver ...")`. Each line is folded into a 64-bit FNV-1a hash, a fingerprint of the entire run. `DeterminismTest` runs a chaotic scenario twice per seed and demands identical fingerprints. If anyone ever adds hidden nondeterminism (a `HashMap` iteration, a `System.nanoTime()` call), this test catches it.
- **Failures carry the seed:** `event failed at t=1.500s (re-run with -Draftkv.seed=8812734)`.

### `SimulatedNetwork`
Every `send` becomes a future delivery event with a random latency, so messages **reorder** naturally. On top of that it can drop, duplicate, partition, isolate, and take nodes down. Two details matter:
1. **Links are checked at send *and* at delivery.** A partition that starts while a message is in flight drops it, like a broken TCP connection.
2. **Every message is really encoded and decoded.** So the production codec runs on every simulated message, and nodes can never accidentally share a mutable object (the test asserts `isNotSameAs`).

---

## 6. How the tests prove it works

| Test | What it proves |
|---|---|
| `BinaryCodecTest.primitivesRoundTrip` | jqwik property: 1000 random combinations of every primitive round-trip exactly |
| `FrameCodecTest.survivesArbitraryFragmentation` | 300 random streams, cut into random chunks (often single bytes), decode to the original frames in order |
| `FrameCodecTest.detectsEverySingleBitFlip` | flipping **any** one bit is detected; the decoder never returns a wrong frame |
| `MessageCodecTest.anyMessageRoundTripsThroughAFrame` | 2000 random messages (biased toward edge values) survive message → frame → bytes → back |
| `MessageCodecTest.everyTruncationIsRejected` | every prefix of ~60 messages is rejected cleanly |
| `MessageCodecTest.lyingEntryCountDoesNotAllocate` | "2 billion entries follow" doesn't crash the node |
| `MessageCodecTest.wireCodesAreStable` | nobody can renumber protocol codes by accident |
| `SimulationTest` | time order, cancellation, `runFor`/`runUntil` semantics, seed-ordered ties |
| `SimulatedNetworkTest` | latency bounds, reordering, drop, duplicate, partitions, in-flight drops, down nodes |
| `DeterminismTest` | **the milestone gate**: same seed ⇒ identical run; 30 seeds ⇒ 30 different runs |

Replay a randomized test with a fixed seed:
```powershell
.\mvnw.cmd test "-Dtest=DeterminismTest" "-Draftkv.seed=8812734"
```

---

## 7. Check yourself

1. Why would a `record` holding a `byte[]` break round-trip tests? How does `Bytes` fix it?
2. A frame's length field says 2,000,000,000. What does `FrameDecoder` do, and why must that happen *before* the bytes arrive?
3. TCP already has a checksum and guarantees in-order delivery. Why add a length prefix and a CRC32C?
4. You add `record Ping(long term) implements Message` to the `permits` list. Which files stop compiling, and why is that good?
5. Why can Raft use one-way messages instead of request/response RPCs? What would go wrong if a late `RequestVoteReply` from a pre-vote were counted as a real vote? (Hint: look at the `preVote` field.)
6. Two events are scheduled for exactly t = 10 ms. Which runs first? Is it always the same?
7. Someone iterates a `HashMap<NodeId, ...>` to decide which peer to message first. Which test would catch it, and how?
8. A message is in flight when `isolate(B)` is called. Does B receive it? Why is that the realistic choice?
