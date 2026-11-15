package dev.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.Role;
import dev.raftkv.shard.ControllerCommand;
import dev.raftkv.shard.ShardConfig;
import dev.raftkv.shard.ShardedKVClient;
import dev.raftkv.shard.Shards;

/**
 * The same Raft, KV and shard code as in the simulator, now on real TCP, real files and real
 * time: three nodes in one JVM, each hosting the controller and two data groups.
 */
class RealClusterTest {

    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @Test
    void bootstrapsAndServesReadsAndWritesAcrossGroups() {
        try (LocalCluster cluster = new LocalCluster(3, 2)) {
            ShardedKVClient client = cluster.newClient(DEADLINE);
            for (int i = 0; i < 100; i++) {
                assertThat(client.put("k" + i, "v" + i)).isEqualTo(Optional.empty());
            }
            assertThat(client.put("k0", "changed")).isEqualTo(Optional.of("v0"));
            assertThat(client.cas("k1", "v1", "swapped")).isTrue();
            assertThat(client.cas("k1", "v1", "again")).isFalse();
            assertThat(client.delete("k2")).isTrue();
            assertThat(client.get("k0")).isEqualTo(Optional.of("changed"));
            assertThat(client.get("k1")).isEqualTo(Optional.of("swapped"));
            assertThat(client.get("k2")).isEqualTo(Optional.empty());
            assertThat(client.get("k99")).isEqualTo(Optional.of("v99"));

            ShardConfig config = client.controller().query(ControllerCommand.Query.LATEST).join();
            assertThat(config.gids().size()).isEqualTo(2);
            assertThat(config.shardsOf(1).size()).isEqualTo(LocalCluster.SHARDS / 2);
            cluster.awaitConverged();
            for (NodeId id : cluster.ids()) {
                assertThat(cluster.keysOn(id)).as("keys on %s", id).isEqualTo(99);
                assertThat(cluster.status(id, GroupId.of(1)).keys()).as("group 1 holds some keys").isGreaterThan(0);
                assertThat(cluster.status(id, GroupId.of(2)).keys()).as("group 2 holds some keys").isGreaterThan(0);
            }
            assertThat(cluster.fatalErrors().isEmpty()).isTrue();
        }
    }

    @Test
    void survivesANodeStoppingAndCatchesItUpWhenItReturns() {
        try (LocalCluster cluster = new LocalCluster(3, 2)) {
            ShardedKVClient client = cluster.newClient(DEADLINE);
            for (int i = 0; i < 20; i++) {
                client.put("before" + i, "x" + i);
            }
            NodeId victim = cluster.awaitLeader(GroupId.of(1));
            cluster.stop(victim);
            for (int i = 0; i < 20; i++) {
                client.put("during" + i, "y" + i);
            }
            assertThat(cluster.awaitLeader(GroupId.of(1))).isNotEqualTo(victim);

            cluster.start(victim); // same port, same data directories: recovers every group from disk
            for (int i = 0; i < 20; i++) {
                client.put("after" + i, "z" + i);
            }
            cluster.awaitConverged();
            for (NodeId id : cluster.ids()) {
                assertThat(cluster.keysOn(id)).as("keys on %s", id).isEqualTo(60);
            }
            for (int i = 0; i < 20; i++) {
                assertThat(client.get("before" + i)).isEqualTo(Optional.of("x" + i));
                assertThat(client.get("during" + i)).isEqualTo(Optional.of("y" + i));
                assertThat(client.get("after" + i)).isEqualTo(Optional.of("z" + i));
            }
            assertThat(cluster.fatalErrors().isEmpty()).isTrue();
        }
    }

    /** Milestone 10 over real TCP: shards move between groups while their data stays readable. */
    @Test
    void shardsMoveBetweenGroupsAndKeepTheirData() {
        try (LocalCluster cluster = new LocalCluster(3, 2)) {
            ShardedKVClient client = cluster.newClient(DEADLINE);
            for (int i = 0; i < 60; i++) {
                client.put("m" + i, "v" + i);
            }
            ShardConfig before = client.controller().query(ControllerCommand.Query.LATEST).join();
            // Move every shard of group 1 to group 2, one Move at a time, while writing.
            for (int shard : before.shardsOf(1)) {
                client.controller().move(shard, 2).join();
                client.put("during-move-" + shard, "w");
            }
            ShardConfig after = client.controller().query(ControllerCommand.Query.LATEST).join();
            assertThat(after.shardsOf(1).isEmpty()).isTrue();
            for (int i = 0; i < 60; i++) {
                assertThat(client.get("m" + i)).as("m%d", i).isEqualTo(Optional.of("v" + i));
            }
            RealNet.await("group 2 serves every shard on every node", 30_000, () -> cluster.ids().stream().allMatch(id ->
                    cluster.status(id, GroupId.of(2)).shardsServing() == LocalCluster.SHARDS
                            && cluster.status(id, GroupId.of(1)).shardsServing() == 0
                            && cluster.status(id, GroupId.of(1)).shardsMoving() == 0));
            cluster.awaitConverged();
            for (NodeId id : cluster.ids()) {
                assertThat(cluster.status(id, GroupId.of(1)).keys()).isEqualTo(0);
                assertThat(cluster.keysOn(id)).isEqualTo(60 + before.shardsOf(1).size());
            }
            // And back: a Join of group 1 again rebalances half the shards to it.
            client.controller().leave(java.util.List.of(1)).join(); // no-op for data, removes the group
            int shardOfM0 = Shards.shardOf(Bytes.ofUtf8("m0"), LocalCluster.SHARDS);
            assertThat(client.controller().query(ControllerCommand.Query.LATEST).join().gidOf(shardOfM0)).isEqualTo(2);
            assertThat(client.get("m0")).isEqualTo(Optional.of("v0"));
            assertThat(cluster.status(cluster.awaitLeader(GroupId.of(2)), GroupId.of(2)).role()).isEqualTo(Role.LEADER);
            assertThat(cluster.fatalErrors().isEmpty()).isTrue();
        }
    }
}
