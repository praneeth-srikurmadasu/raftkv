# Milestone 10: Moving shards safely, explained

When the controller makes a new configuration, some shards change owner. Their data, and their client sessions, must move from the old group to the new one while clients keep running, nodes crash, and messages get lost, duplicated or delayed. Nothing may be lost, nothing may run twice, and no two groups may serve the same shard at the same time. This is the hardest part of the project (MIT 6.5840's ShardKV, including both challenge parts: garbage collection, and serving unaffected shards during a move).

```
src/main/java/dev/raftkv/shard/
├── ShardStatus.java          NOT_OWNED, SERVING, PULLING, BEPULLING, GCING
├── ShardData.java            one shard's keys + its session table
├── ShardOp.java              ConfigChange, InsertShards, DeleteShards, GcDone (log entries)
├── ShardKVStateMachine.java  the replicated state of a data group
└── ShardKVServer.java        config poller, migrator, FetchShard/DeleteShard handling
src/main/java/dev/raftkv/protocol/
└── FetchShard, FetchShardReply, DeleteShard, DeleteShardReply   (ShardMessage)
src/test/java/dev/raftkv/shard/
├── ShardKVStateMachineTest, ShardedClusterTest   scripted migrations and edge cases
├── ShardInvariants.java      SPEC §6.10, checked after every simulated event
├── ShardedChaos(Test)        the gate: 2,000 swarm runs with Join/Leave/Move all the time
├── GcScenario, ShardBugDetectionTest   injected migration bugs must be caught
```

---

## 1. Every shard has a status, and the status is replicated

| Status | Meaning | Serves clients? |
|---|---|---|
| `NOT_OWNED` | not ours | no → WRONG_GROUP |
| `SERVING` | ours, data here | yes |
| `PULLING` | ours now, data still at the previous owner | no → SHARD_NOT_READY |
| `BEPULLING` | ours before, not now; frozen copy kept for the new owner | no → WRONG_GROUP |
| `GCING` | ours, data installed, old owner not yet told to delete | yes |

The statuses live **in the state machine**, changed only by log entries. A new leader knows exactly what's left to do by looking at them. No migration state lives only in a leader's memory, so a leader crash mid-migration loses nothing.

## 2. One configuration at a time

The leader of each data group polls the controller every 100 ms for configuration `current + 1`, and only when every shard is `SERVING` or `NOT_OWNED` (the previous move is finished). It proposes it as a `ConfigChange` log entry. When applied:
- shards gained → `PULLING` (or straight to `SERVING`, empty, if nobody owned them before),
- shards lost → `BEPULLING`,
- everything else keeps serving, uninterrupted.

**Never skip a configuration.** Going from 5 to 7 directly could miss that a shard moved twice; the group would pull it from the wrong place. (`SKIP_CONFIG_NUMBERS` bug, caught at seed 1.)

**Why a log entry?** Every replica switches at the same position in the operation order. An operation before the ConfigChange entry is applied by the old owner; one after it is refused by the old owner. No operation can be applied by both.

## 3. The migration (pull-based)

```
new owner (PULLING)                               old owner (BEPULLING)
   │ FetchShard(config 7, shards [17])  ─────────▶  answers only once it has applied config 7
   │ ◀──────────  shard data + session table        (any replica can answer: frozen = identical)
   │ propose InsertShards(7, {17: data})
   │ applied: PULLING → GCING, starts serving
   │ DeleteShard(7, [17])               ─────────▶  proposes it through its own log
   │                                                applied: BEPULLING → NOT_OWNED, data deleted
   │ ◀──────────  OK (or OK if already past config 7)
   │ propose GcDone(7, [17])
   │ applied: GCING → SERVING
```
Every step names its configuration number and is ignored unless it matches the current state. That makes duplicates, retries and late messages harmless: a second InsertShards for a shard that's already `GCING` does nothing.

Why pull, not push? The receiver knows when it's ready and drives retries; the sender stays passive and simple; crash recovery is "keep polling".

**The garbage-collection order matters.** The old owner may delete only after the new owner's InsertShards is *committed*. If the new owner asked for deletion right after fetching, a leader crash before the insert committed would lose the shard forever: the old copy deleted, the new one never written. (`GC_BEFORE_INSERT_COMMITTED` bug; a scripted scenario that crashes the leader at exactly that moment catches it.)

## 4. Sessions move with the shard

A client sends `PUT x=1` with sequence 42 to group A. A commits it, but the reply is lost. Meanwhile x's shard moves to group B. The client gets WRONG_GROUP on retry, refreshes the config, and retries **sequence 42** at B. If B didn't know about sequence 42, it would run the PUT a second time.

So each shard carries its own session table, and FetchShard sends it with the data. B answers the retry from the cached result. (`SESSIONS_NOT_MIGRATED` bug, caught at seed 2 by the "no double apply" invariant.)

## 5. Check ownership when applying, not only on arrival

A server checks the shard status when a request arrives (to avoid wasting a log entry on a request it will refuse), but that check is not enough: between arrival and commit, a ConfigChange entry may sit in between. The decisive check happens again **when the entry is applied**. (`CHECK_OWNERSHIP_ON_RECEIVE_ONLY` bug, caught at seed 13 by the linearizability checker; `SERVE_WHILE_BEPULLING`, caught at seed 1.)

## 6. Invariants checked after every simulated event

- **Election safety** in every group.
- **Single owner:** for every configuration number, at most one group has a shard SERVING or GCING.
- **Monotonic configs:** a replica's config number only goes up, one at a time (a snapshot may jump several, which the group applied one by one earlier).
- **No double apply:** each (client, sequence) executes at most once across all groups, ever.
- **No lost data:** at quiescence every shard has exactly one holder and every key reads back the value the linearizability model expects.

## 7. The gate

Each of the 2,000 runs per build: 3 data groups plus the controller, 4 to 12 shards (few shards = constant conflicts), every group on its own nodes or all co-located on the same three; swarm faults (partitions, crashes and restarts, disk faults, message loss, duplicates, latency spikes); snapshots on in half the runs; clients doing random PUT/GET/CAS/DELETE; and an admin issuing random Join/Leave/Move the whole time, so migrations overlap with every kind of failure. Every history must be linearizable and every invariant must hold. A 2,500-run stress run: 1.4 million operations and 20,233 reconfigurations, all linearizable.

Edge cases from SPEC §6.7 with scripted tests:
- `aShardWaitsForItsDownedPreviousOwnerWhileOthersServe`: the old owner's whole group is down; the moving shard is unavailable (the price of strong consistency), every other shard keeps serving.
- `dataGroupsServeWhileTheControllerIsDown`: no controller means no new moves, not an outage.
- `migrationSurvivesCrashesDuplicatesAndSnapshots`, `aSnapshotTakenMidMigrationResumesIt`: a snapshot taken mid-move includes statuses, the previous config and frozen data, and a restart resumes the move.
- Shards moving A → B → A, keys moving while a request is in flight, and leader crashes between fetch and insert all happen many times in the chaos runs.

## 8. Try it on your machine

```powershell
.\scripts\run-cluster.ps1 -Clean
.\scripts\cli.ps1
raftkv> put city Hyderabad
raftkv> shard city                 # e.g. shard 12 -> group 1
raftkv> admin move 12 2            # move it; watch the node windows log the migration
raftkv> get city                   # still Hyderabad, now served by group 2
raftkv> admin leave 3              # group 3's shards spread over 1 and 2
raftkv> config
raftkv> admin join 3 n1,n2,n3      # and back
raftkv> quit
```
