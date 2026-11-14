# Milestone 4: Persistence explained

Until now, "durable" state lived in memory objects that the test harness carefully kept across crashes. This milestone puts it on disk for real: a crash-safe file for `currentTerm` / `votedFor`, a segmented log with checksums, and a **simulated disk** that loses or tears everything that wasn't fsynced when the power goes out.

```
io/
├── Disk.java, DiskFile.java     the only file operations storage may use
├── RealDisk.java                FileChannel; directory fsync (a no-op on Windows/NTFS)
└── sim/SimulatedDisk.java       loses / keeps / tears unsynced writes and name changes on crash
raft/storage/FileMetaStore.java  term + vote: tmp file, fsync, atomic rename, dir fsync
raft/log/SegmentedFileLog.java   the log on disk: segments, CRC records, recovery, group commit
raft/RaftNode.java               fsync before acknowledging; leader counts itself only after fsync
```

---

## 1. What "durable" really means

`write()` puts bytes into the operating system's page cache, not on the disk. If the machine loses power, any of these can happen to writes since the last `fsync`:

- **lost**: never reached the disk;
- **kept**: the OS flushed it in time;
- **torn**: only the first part reached the disk (a half-written record);
- **reordered**: a later write survives while an earlier one doesn't, leaving a hole.

File *names* are separate. Creating, renaming or deleting a file changes the **directory**, and that change is durable only after the directory itself is fsynced.

`SimulatedDisk` implements exactly this model. It keeps a durable copy of every file plus a list of pending operations. `crash(RANDOM)` rebuilds each file from its durable copy, and every pending write is independently kept, lost or torn. Every pending create, rename or delete is independently kept or lost. Old file handles die. Storage code must then reopen and recover from whatever survived.

## 2. FileMetaStore: never forget a vote

```
1. write meta.tmp      never overwrite the live file: a crash mid-write = torn garbage
2. fsync meta.tmp      its bytes must be on disk before step 3 makes it official
3. rename → meta       atomic: readers see the complete old file or the complete new one
4. fsync directory     the rename itself is a directory change; without this a crash can
                       undo it and bring back the OLD vote → double vote → two leaders
```

`crashAtAnyStepLeavesTheOldOrTheNewStateNeverAMix` cuts the power before **every** one of those operations, in all three crash modes, for 50 seeds. Recovery always yields the complete old state or the complete new state. `theDirectoryFsyncIsWhatMakesTheRenameStick` shows step 4 matters: crash right before it, and the old vote comes back.

## 3. SegmentedFileLog

```
log/log-00000000000000000001.seg   entries 1..812
log/log-00000000000000000813.seg   entries 813..
record: [length:4][crc32c:4][index:8][term:8][type:1][data:N]
```

Why segments? Snapshots (Milestone 8) let us drop the start of the log. With segments that means deleting whole files, not rewriting one giant file.

An in-memory index keeps `(offset, term)` per entry, so `termAt`, which Raft calls constantly, never touches the disk.

### Recovery
On open, every record is re-read and checked: plausible length, CRC, `index == previous + 1`, term never decreases.
- A bad record in the **last** segment is a torn write from a crash. It's truncated (durably). It was never fsynced, so it was never acknowledged, so nothing promised is lost.
- A bad record in an **earlier** segment, or a gap between segments, is real corruption. The node refuses to start (`CorruptStateException`) rather than guess.

For that split to be valid, damage must only ever be possible in the last segment. Two rules guarantee it, and the stress testing below found that both were needed.

### Durability rules
1. `append()` only writes; `sync()` forces. Raft calls `sync()` before acknowledging.
2. **Rolling to a new segment:** first fsync the old segment's contents **and** the directory (its name was created at the previous roll), then create the next one.
3. **Truncation is durable on its own:** delete later segments **newest first, syncing the directory after each**, then truncate the file and fsync it. Only then can new entries be written, so a crash can never mix old and new entries.

### Two bugs found by the crash-at-any-moment property

`recoversToAValidStateAfterACrashAtAnyMoment` runs a random mix of appends, syncs and truncations, cuts the power at a random disk operation, crashes in a random mode, and checks three things: recovery succeeds, nothing synced is lost, and every recovered entry is one that was really written at that index. It found two real bugs in the first version:

1. **Rolling without syncing the directory.** Segment contents were forced at each roll, but their *names* only became durable at the next `sync()`. A crash kept segment 37's name, lost 25's and 31's, and the node refused to start because of the gap. Fix: rule 2.
2. **Batch delete during truncation.** Truncating deleted seven segments, then synced the directory once. A crash undid some deletions and kept others, leaving segments 14, 32 and 50 with holes between them. Fix: rule 3, newest first, one directory sync per delete.

After both fixes, 100,000 random seeds of that property pass. This is the point of a simulated disk: on a real machine these bugs would show up once in a blue moon as a node that won't start.

## 4. Raft's side of durability

**Followers fsync before replying.** A success reply promises the leader: "these entries survive a crash". So a follower syncs whenever the log changed, **and** whenever the reply covers entries it appended earlier but never synced. That second case can happen: a node is briefly leader, appends an entry, steps down before its flush runs, and the new leader sends that same entry. "Nothing new to append" would otherwise acknowledge a non-durable entry.

**The leader writes in parallel (thesis §10.2.1).** A proposal is appended and sent to followers *immediately*, and the leader's own fsync runs in a scheduled `flush()`. The leader counts **its own copy** toward a majority only once that flush completes (`durableIndex`). Followers alone can still form a majority, which is fine.

**Group commit.** `flush()` is scheduled once; every append made before it runs shares the same fsync. `manyProposalsShareOneFsync`: 100 proposals, one fsync. fsync is the slowest operation in the system (milliseconds on SSDs, more on disks), so this is the single biggest throughput lever. Milestone 13 benchmarks it.

## 5. Testing

| Test | Proves |
|---|---|
| `SimulatedDiskTest` (10) | the disk model itself: forced data survives, unforced can be lost/kept/torn, names need dir sync, renames are atomic, handles die |
| `FileMetaStoreTest` (6) | crash before every step × every mode × 50 seeds → old or new, never a mix; corruption is fatal |
| `RaftLogContractTest` (×2) | the same 6 behaviors for the in-memory and the on-disk log (every reopen is a crash) |
| `SegmentedFileLogTest` (7) | rolling, truncation across segments, torn tails, fatal corruption/gaps, crash-at-any-moment property |
| `RealDiskTest` | the real file system works on your OS |
| `PersistenceTest` (5) | **gate:** all nodes lose power at once, 3 times, with writes in flight; every acknowledged write survives. Also term/vote memory, group commit, follower recovery |
| `PersistenceChaosTest` | 500 chaos runs where **every crash loses or tears unsynced disk data** |

Bug detection: `REPLY_BEFORE_FSYNC` (acknowledge without fsync) is caught by disk chaos at seed 4 and by the all-nodes-crash scenario at seed 1.

## 6. Platform notes

- **Windows:** a directory can't be opened as a `FileChannel`, so `syncDirectory` is a no-op. NTFS journals metadata, so renames are durable without it.
- **Linux:** `force(false)` is `fdatasync`. It flushes data plus the metadata needed to read it back (including file size), so it covers appends and truncations.
- **Known limitation:** a bit flip *inside the last segment* looks the same as a torn tail and is truncated there instead of being reported (etcd's WAL behaves the same way). If that node had acknowledged those entries, Raft still has them on a majority and re-replicates them.

## 7. Check yourself

1. Why does FileMetaStore write a temp file and rename it instead of overwriting `meta` in place?
2. Which step of the save protocol is skipped in the "forgot my vote" scenario, and what exactly goes wrong in Raft?
3. A follower appends entries and replies *before* fsync. Construct a crash scenario where a committed entry disappears.
4. Why must the leader not count its own copy until its fsync finishes, while it may still send the entries to followers before that?
5. Explain why rolling to a new segment needs a directory fsync. What did the crash look like without it?
6. During truncation, why delete segments newest-first with a sync after each, instead of deleting them all and syncing once?
7. Recovery finds a bad CRC. When does it truncate, and when does it refuse to start? Why is that split safe?
8. What is group commit, and why is it the biggest throughput lever in a replicated log?
