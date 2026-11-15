# Milestone 9: Multi-Raft and the shard controller, explained

One Raft group can only go as fast as its leader. To scale, split the keys into **shards** and give different shards to different Raft groups. Each group is a complete Raft cluster with its own leader, log and snapshots; many groups share the same server processes (**multi-Raft**). A special group, the **controller**, decides which group owns which shard.

```
src/main/java/dev/raftkv/
├── shard/Shards.java               key → shard (FNV-1a 64 of the key bytes, mod N)
├── shard/ShardConfig.java          one numbered configuration: shard → gid, gid → servers
├── shard/Rebalancer.java           even spread with the fewest possible moves (pure function)
├── shard/ControllerCommand.java    Join, Leave, Move, Query
├── shard/ControllerStateMachine.java   group 0's state: every configuration ever made
├── shard/ControllerClient.java     async client for the controller (admin tools, config pollers)
├── shard/ShardedKVClient.java      routes key → shard → group → leader
├── server/MessageRouter.java       per process: envelope → that group's replica, on its own thread
├── server/Replicas.java, GroupReplica.java   builds a controller or data-group replica
├── config/ClusterBootstrap.java    gives a brand-new cluster its first configuration
└── client/SessionClient.java, Router.java, Route.java   the session/retry core shared by all clients
```

---

## 1. Shards

There are **N fixed shards** (default 64, chosen once, never changed): `shard = fnv1a64(key) mod N`. A key never changes shard; only the shard → group assignment changes. That's what makes moving data manageable: the unit of movement is a whole shard, not individual keys.

Why not consistent hashing? With fixed virtual shards the controller has full, explicit control (move shard 17 to group 4) and every replica can compute ownership from one small table. Why not key ranges? Ranges allow range scans and splitting hot ranges, but need split/merge logic; that's a listed stretch goal.

## 2. The controller (group 0)

Its state machine is just a list of configurations. `ShardConfig(num, shard → gid, gid → servers)`. Configuration 0 has no groups and every shard on gid 0, which means "unassigned".

| Command | Effect |
|---|---|
| `Join({gid → servers})` | new config with the new groups, rebalanced |
| `Leave([gid])` | new config without those groups; their shards go to the others |
| `Move(shard, gid)` | new config with just that shard moved, no rebalance |
| `Query(num)` | config `num`, or the latest for -1 |

Commands go through the log with the same exactly-once sessions as the KV store, so a retried Join doesn't make two configurations (`aRetriedJoinCreatesOneConfiguration`). It refuses a Leave that would remove the last group, and a Move to a group that doesn't exist.

## 3. The rebalancer

Given the current assignment and the set of groups, produce an assignment where every group has ⌊N/G⌋ or ⌈N/G⌉ shards, **moving as few shards as possible**, deterministically (every controller replica must compute the same answer).

1. Shards on gid 0 or on departed groups are free.
2. `N mod G` groups get one extra shard: the ones holding the most right now (ties: lowest gid), so the most shards stay put.
3. Groups above their target release their highest-numbered shards.
4. Groups below their target, lowest gid first, take free shards, lowest-numbered first.

Every move is forced (the shard was free or surplus), so the count is minimal. `RebalancerTest` checks this with jqwik over random histories of joins and leaves: balanced, minimal, deterministic, nothing on a departed group.

Example from the demo: 3 groups, 64 shards → 22/21/21. Group 3 leaves → its 21 shards split 11/10 between groups 1 and 2, and nothing else moves. Group 3 joins again → exactly those 21 come back.

## 4. Many groups in one process

Every server process runs one `RaftHost` with a replica of the controller and of each data group (`--groups`, default 3). Each replica has its own event loop thread, log directory, metadata file and snapshot:

```
data/n1/group-0/   controller
data/n1/group-1/   data group 1 (log/, meta, snapshot)
data/n1/group-2/   ...
```
`MessageRouter` reads the group id in each incoming frame and hands the message to that group's thread. Client replies are routed by client id to in-process clients (the config pollers). The **same router and replica builders run in the simulator**, so the routing code is tested under chaos too.

Per-group logs are simple and isolated but cost one fsync stream per group. Production systems like TiKV share one write-ahead log across groups to batch fsyncs; that's future work.

## 5. Routing clients

`ShardedKVClient` starts knowing only the server addresses. It fetches the latest configuration from the controller and caches it, then for each operation: key → shard → gid → that group's (cached) leader.

| Reply | Client does |
|---|---|
| `NOT_LEADER` | follow the hint, or try the next server |
| `WRONG_GROUP` | the shard moved: refresh the config, retry at the new owner |
| `SHARD_NOT_READY` | the new owner is still pulling the data: back off, retry the same group |
| timeout | try the next server, eventually refresh the config |

Every retry keeps the **same sequence number**, even when it goes to a different group. Milestone 10 explains why that is safe.

## 6. Starting a brand-new cluster

The controller starts at config 0, with no groups, so nobody serves anything. `ClusterBootstrap` runs in every process, but only the **lowest-id node** acts: once a second it asks the controller whether a configuration with groups exists, and if not, sends one Join with all data groups. The Join goes through a session, and the controller refuses to join a group twice, so the cluster gets exactly one initial configuration even if that node restarts mid-way. (If the lowest-id node never starts, a new cluster waits for it.)

## 7. Try it

```powershell
.\scripts\run-cluster.ps1 -Clean
.\scripts\cli.ps1 config          # config 1: group 1 -> shards 0-21, group 2 -> 22-42, group 3 -> 43-63
.\scripts\cli.ps1 put city Hyderabad
.\scripts\cli.ps1 shard city      # which shard and group serve it
```
