package dev.raftkv.shard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;

/**
 * A scripted SPEC §6.7 scenario: the new owner's leader fetches a shard but loses its majority
 * before the InsertShards entry commits, and a new leader has to finish the migration.
 *
 * <ol>
 *   <li>Group 1 owns every shard and holds some keys. Group 2 joins, but group 1 is cut off, so
 *       group 2 applies the new configuration and waits with shards PULLING.</li>
 *   <li>Group 2's followers are cut off and group 1 comes back: group 2's leader fetches the shards
 *       and proposes the insert, which cannot commit.</li>
 *   <li>That leader is cut off and its followers come back: they elect a new leader, which must
 *       fetch the shards again from group 1.</li>
 * </ol>
 * Correct code only asks group 1 to delete its frozen copy once the insert is committed (GCING),
 * so the new leader finds the data and the cluster settles with every key intact. With
 * {@code Bug.GC_BEFORE_INSERT_COMMITTED}, group 1 already deleted it: the shards stay PULLING forever.
 *
 * @return empty if the cluster settled with all data, otherwise what went wrong
 */
final class GcScenario {

    private GcScenario() {
    }

    static Optional<String> run(long seed, RaftConfig config) {
        ShardedCluster c = new ShardedCluster(seed, 2, 6, config, ShardedCluster.Storage.MEMORY, false);
        c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(3));
        c.startAll();
        ClientConfig cc = new ClientConfig(Duration.ofMillis(200), Duration.ofMillis(10), Duration.ofMillis(200),
                Duration.ofSeconds(20));
        ControllerClient admin = c.newAdmin(cc);
        ShardedKVClient client = c.newClient(cc);
        try {
            c.await(admin.join(c.groupsFor(1)), Duration.ofSeconds(20));
            for (int i = 0; i < 20; i++) {
                c.await(client.putAsync(Bytes.ofUtf8("k" + i), Bytes.ofUtf8("v" + i)), Duration.ofSeconds(20));
            }
            List<NodeId> g1 = c.groupNodes.get(1);
            List<NodeId> g2 = c.groupNodes.get(2);

            // 1. Group 1 cut off; group 2 joins and starts PULLING.
            c.net.partition(g1);
            c.await(admin.join(c.groupsFor(2)), Duration.ofSeconds(20));
            if (!c.sim.runUntil(() -> c.replicas(GroupId.of(2)).stream().allMatch(r ->
                    !((ShardKVStateMachine) r.stateMachine()).shardsIn(ShardStatus.PULLING).isEmpty()), Duration.ofSeconds(10))) {
                return Optional.of("group 2 never started pulling");
            }
            RaftNode leader = c.leader(GroupId.of(2)).orElseThrow();
            List<NodeId> followers = new ArrayList<>(g2);
            followers.remove(leader.id());

            // 2. Group 2's leader keeps group 1 but loses its own followers: fetches, can't commit.
            c.net.partition(followers);
            c.run(Duration.ofMillis(400));

            // 3. The leader is cut off; its followers elect a new one, which must finish the job.
            c.net.partition(List.of(leader.id()));
            c.run(Duration.ofSeconds(2));
            c.net.heal();
            c.awaitQuiescent(Duration.ofSeconds(30));
            c.checkSettledOwnership();
            for (int i = 0; i < 20; i++) {
                Optional<Bytes> v = c.await(client.getAsync(Bytes.ofUtf8("k" + i)), Duration.ofSeconds(20));
                if (!v.equals(Optional.of(Bytes.ofUtf8("v" + i)))) {
                    return Optional.of("k" + i + " lost: " + v);
                }
            }
            return Optional.empty();
        } catch (AssertionError | RuntimeException e) {
            return Optional.of(String.valueOf(e.getMessage()));
        }
    }
}
