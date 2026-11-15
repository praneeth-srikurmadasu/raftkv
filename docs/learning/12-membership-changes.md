# Milestone 12: Changing who is in the group, explained

A Raft group's membership was fixed at startup. Real systems replace machines, grow and shrink. This milestone adds single-server membership changes (thesis chapter 4), learners, removing the leader itself, and leadership transfer, plus the 2015 safety fix that the original single-server algorithm missed.

```
src/main/java/dev/raftkv/raft/
├── Membership.java        voters + learners; one-server-at-a-time rule; encoding
├── RaftNode.java          configuration entries, addServer / removeServer / changeMembership,
│                          transferLeadership, TimeoutNow, learners never campaign
src/main/java/dev/raftkv/server/Admin.java        status / add / remove / transfer over the wire
src/main/java/dev/raftkv/client/AdminClient.java  the client side (used by the CLI)
src/test/java/dev/raftkv/raft/
├── MembershipTest.java         learners, 3→5→3 under writes, removal of the leader, transfer,
│                               snapshots, and the 2015 bug reproduced step by step
└── MembershipChaosTest.java    the gate: 300 swarm runs of membership churn + faults + clients
```

---

## 1. A configuration is a log entry, and it takes effect when appended

The membership lives in the log as a `CONFIGURATION` entry. Each server uses **the latest configuration in its log, committed or not**. If that entry is later truncated (a new leader overwrote it), the server goes back to the previous one. This is the thesis' rule, and it's what makes changes safe without a two-phase "joint consensus".

Snapshots carry the membership in effect at their index, so a configuration survives compaction (`membershipSurvivesSnapshotsAndRestarts`).

## 2. Why one server at a time is safe

With voters {1,2,3} a majority is 2; with {1,2,3,4} it is 3. Any 2 of the first set and any 3 of the second share at least one server. That one server can't vote for two different leaders in the same term, so even while servers disagree about which configuration is current, two leaders can never be elected in one term. Change two servers at once ({1,2,3} → {1,2,3,4,5}) and {1,2} vs {3,4,5} are disjoint majorities. `Membership.isSingleServerChangeTo` refuses that.

## 3. Learners first

A brand-new server has an empty log. Made a voter right away, it would count toward majorities while it can't store anything yet: commits could stall. So `addServer(id)`:
1. adds it as a **learner**: it receives the log (or a snapshot) but doesn't vote and isn't counted;
2. promotes it to voter once it has caught up with the commit index.

Learners never start elections. Spare processes (started with `--voters 1,2,3` while listed in `--peers`) host every group as learners until an admin adds them.

## 4. The 2015 bug and its fix

Diego Ongaro (Raft's author) reported in 2015 that single-server changes as published could lose committed data. `theMembershipChangeSafetyRuleIsEnforcedAndItsBugIsCaught` replays it:

1. Voters {1,2,3,4}, node 5 a learner. Leader A appends "make 5 a voter" ({1,2,3,4,5}); it reaches only node 5.
2. Cut off from A, the others elect B. B immediately removes one of them, R, giving {1,2,3,4}−R, before B's own no-op has committed. The removal commits on B and the remaining M (2 of the 3 voters of the new configuration).
3. A, R and 5 get together: under A's configuration ({1,..,5}) they're a majority. R never saw B's entries, so it votes for A, who wins and **overwrites the committed removal**.

The fix: **a leader may not change the membership until an entry of its own term has committed**. B's no-op would need a majority of the *old* configuration, which includes R or A, and then A could never win. Our implementation doesn't refuse; the change simply waits until that has happened. Also one change at a time: a second change waits for the first to commit.

With `Bug.CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT` the scenario overwrites a committed entry and the invariant checker stops the run (caught at seed 2). With the rule, no seed out of 30 does.

## 5. Removing a server, even the leader

The removed server is excluded from majorities as soon as the entry is appended. The leader keeps sending it entries until the removal commits, so that it learns it's out (and stops campaigning) instead of being silently abandoned. Then it is forgotten.

A leader can remove itself. It keeps leading until the change commits, without counting itself, then steps down and becomes a learner (`aRemovedLeaderStepsDownOnceTheChangeCommits`).

## 6. Leadership transfer (thesis §3.10)

`transferLeadership(target)`: the leader stops accepting new work, brings the target's log up to date, then sends it `TimeoutNow`. The target starts an election at once, skipping pre-vote, with a flag that tells voters to ignore stickiness. Transfers usually finish within milliseconds. If nothing happens within an election timeout, the transfer is abandoned and the leader carries on. During a transfer the leader gives up its lease (stickiness is being bypassed, so a lease would be unsafe).

## 7. The gate: churn under chaos

`MembershipChaosTest`: 5 servers, starting with voters {1,2,3}. An admin keeps growing the group to 5 voters and shrinking it back to 3, through whichever node leads, while clients run the usual workload and the swarm nemesis crashes nodes (with disk faults in half the runs), partitions the network and loses, duplicates and delays messages. 300 runs per build; every history must be linearizable and every Raft invariant must hold. A typical run commits around 40 configuration changes.

## 8. Doing it on a real cluster

```powershell
.\scripts\run-cluster.ps1 -Clean -Spares 2   # n1-n3 vote; n4, n5 are spares
.\scripts\start-node.ps1 -Id 4
.\scripts\cli.ps1
raftkv> admin add-server 1 n4      # learner, caught up, promoted
raftkv> admin transfer-leader 1 n4
raftkv> admin remove-server 1 n1
raftkv> status                     # every server's view of every group
```

## 9. Not done (see docs/design.md, Deviations)

- Membership chaos runs on the single-group store. Sharded clusters use the same code per group, and the CLI can change any group's membership, but no chaos test changes a data group's membership *while shards migrate*.
- The controller's configurations still list the servers a group started with. A client finds a moved leader by following NOT_LEADER hints, as long as the new server is in its `--servers` list.
