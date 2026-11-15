# Milestone 8: Snapshots and log compaction, explained

Up to Milestone 7 the log only grew. A node that restarted replayed every command since the cluster was born, and a follower that fell far behind needed every entry it missed. Raft §7 fixes both with **snapshots**: write down the state machine's complete state at some index, then throw away the log up to that index.

```
src/main/java/dev/raftkv/raft/
├── storage/Snapshot.java            (lastIncludedIndex, lastIncludedTerm, data)
├── storage/SnapshotStore.java       save() / load()
├── storage/FileSnapshotStore.java   one crash-safe file per group
├── storage/InMemorySnapshotStore.java
├── log/RaftLog.java                 + snapshotIndex/snapshotTerm, compactPrefix, reset
└── RaftNode.java                    takes snapshots, sends InstallSnapshot, installs them
src/test/java/dev/raftkv/raft/
├── SnapshotTest.java                scripted scenarios (catch-up, restart, lossy network, crashes)
├── SnapshotSwarm.java               turns snapshots on, at random sizes, in 2/3 of every chaos run
└── storage/FileSnapshotStoreTest    crash at every step, corruption
```

---

## 1. Taking a snapshot

After applying an entry, the node checks: are there more than `snapshotThreshold` (default 10,000) applied entries that no snapshot covers yet? If so:

1. `stateMachine.snapshot()` serializes the complete state at `lastApplied`. For the KV store that's every key, every client session (so exactly-once survives a restart), and the log time used for session expiry.
2. The snapshot is **saved durably** (below).
3. **Only then** the log prefix up to that index is discarded with `log.compactPrefix(index, term)`.

The order is the whole point. If we deleted entries first and crashed before the snapshot was on disk, those committed entries would be gone from this node.

The log remembers the term of its last discarded entry (`snapshotTerm`). That's what the AppendEntries consistency check needs for the first entry after the snapshot: `prevLogIndex = snapshotIndex` still has a known term, exactly as index 0 had term 0 before.

## 2. Saving it without ever losing it

`FileSnapshotStore` uses the same trick as the metadata file in Milestone 4:

```
write snapshot.tmp  →  fsync it  →  rename over snapshot  →  fsync the directory
```
A crash before the rename leaves the old snapshot; after it, the new one. Never half of one. The file is `["RKVS"][version][index][term][length][data][crc32c]`; a CRC mismatch is fatal, because the log entries the snapshot replaced are already deleted and there's nothing to fall back to. `FileSnapshotStoreTest` crashes the simulated disk at every single operation of a save and checks that either the old or the new snapshot survives.

## 3. Restarting from a snapshot

On startup the node loads the snapshot, restores the state machine from it, and sets `commitIndex = lastApplied = lastIncludedIndex`. Then it looks at its log:
- if the log contains the snapshot's last entry with the same term, it keeps the entries after it;
- otherwise (the log is behind, or conflicts) it discards the whole log and starts empty right after the snapshot.

Log entries after the snapshot are re-applied once a leader tells the node they're committed, as before.

## 4. InstallSnapshot: catching up a follower that is too far behind

The leader keeps `nextIndex` per follower. If `nextIndex - 1` has already been compacted away, AppendEntries can't work: there is no entry left to send. The leader sends its latest snapshot instead (Figure 13):

- The snapshot goes in chunks of `snapshotChunkBytes` (default 64 KiB), each with its byte offset.
- The leader sends the next chunk only after the follower acknowledges the previous one (`InstallSnapshotReply` carries the next offset it wants). Same rule as AppendEntries flow control: send more only on progress, so a lossy network can't make the leader flood the follower.
- The follower accepts chunks strictly in order. A first chunk (offset 0) starts a new transfer; a chunk at the wrong offset gets a reply saying which offset it wants.
- On the last chunk it saves the snapshot durably, then: if its log has an entry matching `(lastIncludedIndex, lastIncludedTerm)`, it keeps what follows; otherwise it discards the whole log. It restores the state machine and sets `commitIndex = lastApplied = lastIncludedIndex`.
- A snapshot older than what the follower has already applied is ignored. State never goes backwards.

## 5. Tests

| Test | What it proves |
|---|---|
| `everyNodeSnapshotsAndCompactsItsLog` | logs stay short; state is identical everywhere |
| `aLaggingFollowerCatchesUpThroughASnapshot` | a follower that was down while 150 entries were written (and compacted away) catches up via InstallSnapshot, over 20 seeds |
| `installSnapshotSurvivesALossyNetwork` | 7-byte chunks, 20% message loss and 20% duplicates: the transfer still completes |
| `nodesRestartFromTheirSnapshotOnDisk` | restart = load snapshot + replay the short tail |
| `allNodesCrashWithSnapshotsOnAndNothingAcknowledgedIsLost` | every node crashes at once, with torn and lost unsynced writes |
| `kvStoreIsExactlyOnceAcrossSnapshots` | sessions survive snapshots: a retry after a restart is answered from the cache |
| `snapshotTransferDoesNotMultiplyMessages` | regression guard for the message-amplification bug of Milestone 3 |

And the important one: **every chaos suite now runs with snapshots on** in two thirds of its seeds (`SnapshotSwarm`), at random thresholds of 5 to 100 entries and chunk sizes of 256 B to 8 KB. Compaction and InstallSnapshot happen constantly while nodes crash, partitions come and go, and messages get lost. The 1,000-run linearizability gate passes with them, and a 4,000-run stress run did too.

## 6. What the extra seeds found

Two failures showed up only in 4,000-seed stress runs (the per-build gates use fresh random seeds, so every build explores new runs). Both were in the **tests**, not in Raft, and both are worth understanding:

- *"marker did not commit on a healthy network"*: a leader that had been partitioned for a long time brought ~380 entries nobody else had; with batches of 1 entry and one AppendEntries in flight per follower, committing took ~8 s of virtual time, and the check only waited 5 s. Liveness means "eventually", so the wait is now 60 s, like the catch-up check next to it.
- *"n1 executed 848 commands, clients completed 849"*: it looked like a lost write. The trace (replay with `-Draftkv.seed=-8730329895450152434 -Draftkv.trace=true`) showed the old leader had committed and acknowledged entry 910, then crashed; the new leader had entry 910 too, but a new leader can't mark anything committed until its own no-op commits, and the test compared replicas in that brief window. The "quiescent" check now also requires the leader to have committed its **whole** log.

## 7. What's simpler than SPEC (see docs/design.md, Deviations)

- The snapshot is serialized **on the event loop**, so the group pauses for the time it takes to copy its state (milliseconds for megabytes). SPEC asks for a copy-on-write capture and serialization on a background thread; that belongs with the performance work in Milestone 11.
- A follower collects incoming chunks **in memory** and writes the complete snapshot through the same crash-safe `save` at the end, instead of appending each chunk to a temp file. Memory use is bounded by the snapshot size.
