package dev.raftkv.shard;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.Role;
import dev.raftkv.server.GroupReplica;

/**
 * The sharding invariants of SPEC §6.10 (plus Election Safety per group), checked after every
 * simulated event:
 * <ul>
 *   <li><b>Election Safety:</b> at most one leader per term in each group.</li>
 *   <li><b>Single owner:</b> for every configuration number, at most one group has a shard
 *       SERVING or GCING under that number.</li>
 *   <li><b>Monotonic configs:</b> a replica's configuration number never decreases, and every
 *       applied ConfigChange advances it by exactly 1 (a restored snapshot may carry the state
 *       forward several configurations at once: the group applied them one by one earlier).</li>
 *   <li><b>No double apply:</b> each (client, sequence) executes at most once across all groups
 *       and all time (re-executing the same log entry after a restart is the same execution).</li>
 * </ul>
 * "No lost data" is checked at quiescence by {@link ShardedCluster#checkSettledOwnership} and by
 * the final linearizable reads of every key.
 */
final class ShardInvariants implements Runnable {

    private record Leadership(GroupId group, long term) {}

    private record OwnerKey(long configNum, int shard) {}

    private record Execution(int gid, long index) {}


    private final ShardedCluster cluster;
    private final Map<Leadership, NodeId> leaders = new HashMap<>();
    private final Map<OwnerKey, Integer> owners = new HashMap<>();
    private final Map<String, Execution> executions = new HashMap<>();
    private final Map<ShardKVStateMachine, Long> configs = new IdentityHashMap<>();
    private long checks;

    ShardInvariants(ShardedCluster cluster) {
        this.cluster = cluster;
    }

    /** Called for every data-group state machine the cluster builds (each replica, each run). */
    void watch(ShardKVStateMachine sm) {
        int gid = sm.gid();
        sm.setConfigObserver((from, to) -> {
            if (to.num() != from.num() + 1) {
                throw new AssertionError("Monotonic configs violated: g" + gid + " applied config " + to.num()
                        + " right after " + from.num());
            }
        });
        sm.setExecutionObserver((UUID client, long seq, long index) -> {
            String key = client + "#" + seq;
            Execution now = new Execution(gid, index);
            Execution earlier = executions.putIfAbsent(key, now);
            if (earlier != null && !earlier.equals(now)) {
                throw new AssertionError("No double apply violated: " + key + " executed by g" + earlier.gid()
                        + " at index " + earlier.index() + " and again by g" + gid + " at index " + index);
            }
        });
    }

    @Override
    public void run() {
        checks++;
        for (Map<GroupId, GroupReplica> node : cluster.allRunning()) {
            for (GroupReplica r : node.values()) {
                RaftNode n = r.node();
                if (n.role() == Role.LEADER) {
                    NodeId earlier = leaders.putIfAbsent(new Leadership(r.group(), n.currentTerm()), n.id());
                    if (earlier != null && !earlier.equals(n.id())) {
                        throw new AssertionError("Election Safety violated in " + r.group() + ": " + earlier + " and "
                                + n.id() + " both lead term " + n.currentTerm());
                    }
                }
                if (r.stateMachine() instanceof ShardKVStateMachine sm) {
                    checkConfig(sm, n.id());
                    checkOwners(sm);
                }
            }
        }
    }

    private void checkConfig(ShardKVStateMachine sm, NodeId node) {
        long num = sm.current().num();
        Long before = configs.put(sm, num);
        if (before != null && num < before) {
            throw new AssertionError("Monotonic configs violated: g" + sm.gid() + " on " + node + " went from config "
                    + before + " back to " + num);
        }
    }

    private void checkOwners(ShardKVStateMachine sm) {
        long num = sm.current().num();
        for (int s = 0; s < sm.numShards(); s++) {
            if (sm.status(s).serves()) {
                Integer earlier = owners.putIfAbsent(new OwnerKey(num, s), sm.gid());
                if (earlier != null && earlier != sm.gid()) {
                    throw new AssertionError("Single owner violated: shard " + s + " served by g" + earlier + " and g"
                            + sm.gid() + " in config " + num);
                }
            }
        }
    }

    long checks() {
        return checks;
    }

    int executionsSeen() {
        return executions.size();
    }
}
