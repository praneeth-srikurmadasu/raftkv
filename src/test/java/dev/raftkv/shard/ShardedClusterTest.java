package dev.raftkv.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/**
 * Milestones 9 and 10 in the simulator: the controller, routing across groups, and shard moves,
 * including the edge cases of SPEC §6.7.
 */
class ShardedClusterTest {

    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final ClientConfig CLIENT = new ClientConfig(Duration.ofMillis(300), Duration.ofMillis(10),
            Duration.ofMillis(300), Duration.ofSeconds(30));

    private static ShardedCluster cluster(long seed, int groups, boolean colocated) {
        ShardedCluster c = new ShardedCluster(seed, groups, 10, RaftConfig.defaults(), ShardedCluster.Storage.MEMORY, colocated);
        c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(5));
        c.startAll();
        return c;
    }

    private static Bytes b(String s) {
        return Bytes.ofUtf8(s);
    }

    private static void putAll(ShardedCluster c, ShardedKVClient client, String prefix, int n) {
        List<CompletableFuture<?>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(client.putAsync(b(prefix + i), b("v" + i)));
        }
        c.sim.runUntil(() -> fs.stream().allMatch(CompletableFuture::isDone), WAIT);
        for (CompletableFuture<?> f : fs) {
            assertThat(f.isDone() && !f.isCompletedExceptionally()).as("seed %d: put failed", c.sim.seed()).isTrue();
        }
    }

    private static void checkAll(ShardedCluster c, ShardedKVClient client, String prefix, int n) {
        for (int i = 0; i < n; i++) {
            assertThat(c.await(client.getAsync(b(prefix + i)), WAIT)).as("seed %d: %s%d", c.sim.seed(), prefix, i)
                    .isEqualTo(Optional.of(b("v" + i)));
        }
    }

    /** The Milestone 9 gate: KV works across three groups with a fixed assignment. */
    @Test
    void keysSpreadOverThreeGroupsAndEveryGroupServesItsShare() {
        for (long seed = 1; seed <= 5; seed++) {
            ShardedCluster c = cluster(seed, 3, seed % 2 == 0);
            ControllerClient admin = c.newAdmin(CLIENT);
            ShardConfig config = c.await(admin.join(c.groupsFor(1, 2, 3)), WAIT);
            assertThat(config.num()).isEqualTo(1L);
            for (int gid = 1; gid <= 3; gid++) {
                assertThat(config.shardsOf(gid).size()).as("balanced").isGreaterThanOrEqualTo(3);
                assertThat(config.shardsOf(gid).size()).as("balanced").isLessThanOrEqualTo(4);
            }
            ShardedKVClient client = c.newClient(CLIENT);
            putAll(c, client, "k", 60);
            checkAll(c, client, "k", 60);
            c.awaitQuiescent(WAIT);
            c.checkSettledOwnership();
            for (int gid = 1; gid <= 3; gid++) {
                ShardKVStateMachine sm = (ShardKVStateMachine) c.replicas(GroupId.of(gid)).get(0).stateMachine();
                assertThat(sm.executedCommands()).as("seed %d: g%d did some of the work", seed, gid).isGreaterThan(5L);
            }
        }
    }

    @Test
    void controllerSemanticsJoinLeaveMoveQuery() {
        ShardedCluster c = cluster(Seeds.resolve(), 3, false);
        ControllerClient admin = c.newAdmin(CLIENT);
        assertThat(c.await(admin.query(-1), WAIT).num()).isEqualTo(0L);
        ShardConfig c1 = c.await(admin.join(c.groupsFor(1)), WAIT);
        assertThat(c1.shardsOf(1).size()).isEqualTo(10);
        ShardConfig c2 = c.await(admin.join(c.groupsFor(2)), WAIT);
        assertThat(c2.shardsOf(2).size()).isEqualTo(5);
        assertThat(Rebalancer.moves(c1.assignment(), c2.assignment())).as("minimal moves").isEqualTo(5);
        ShardConfig c3 = c.await(admin.move(0, 1), WAIT);
        assertThat(c3.gidOf(0)).isEqualTo(1);
        ShardConfig c4 = c.await(admin.leave(List.of(1)), WAIT);
        assertThat(c4.shardsOf(2).size()).isEqualTo(10);
        assertThat(c.await(admin.query(2), WAIT)).isEqualTo(c2);
        assertThat(c.await(admin.query(99), WAIT)).isEqualTo(c4);
        CompletableFuture<ShardConfig> leaveAll = admin.leave(List.of(2));
        c.sim.runUntil(leaveAll::isDone, WAIT);
        assertThat(leaveAll.isCompletedExceptionally()).as("removing every group is refused").isTrue();
    }

    /** The full migration path: join groups one by one under load, then move shards back and forth. */
    @Test
    void shardsMigrateUnderLoadAndKeepTheirData() {
        for (long seed = 1; seed <= 5; seed++) {
            ShardedCluster c = cluster(seed, 3, seed % 2 == 1);
            ControllerClient admin = c.newAdmin(CLIENT);
            ShardedKVClient client = c.newClient(CLIENT);
            c.await(admin.join(c.groupsFor(1)), WAIT);
            putAll(c, client, "a", 40);
            c.await(admin.join(c.groupsFor(2)), WAIT);
            putAll(c, client, "b", 40);
            c.await(admin.join(c.groupsFor(3)), WAIT);
            putAll(c, client, "c", 40);
            // A -> B -> A: shard 0 moves away and back.
            int home = c.controllerConfig().gidOf(0);
            int away = home == 1 ? 2 : 1;
            c.await(admin.move(0, away), WAIT);
            c.await(admin.move(0, home), WAIT);
            c.await(admin.leave(List.of(2)), WAIT);
            checkAll(c, client, "a", 40);
            checkAll(c, client, "b", 40);
            checkAll(c, client, "c", 40);
            c.awaitQuiescent(WAIT);
            c.checkSettledOwnership();
            assertThat(c.controllerConfig().shardsOf(2).isEmpty()).isTrue();
        }
    }

    /** SPEC §6.7: the previous owner's whole group is down: the shard waits; other shards keep serving. */
    @Test
    void aShardWaitsForItsDownedPreviousOwnerWhileOthersServe() {
        long seed = Seeds.resolve();
        ShardedCluster c = cluster(seed, 2, false);
        ControllerClient admin = c.newAdmin(CLIENT);
        ShardedKVClient client = c.newClient(CLIENT);
        c.await(admin.join(c.groupsFor(1, 2)), WAIT);
        putAll(c, client, "k", 40);
        ShardConfig config = c.controllerConfig();
        int movingShard = config.shardsOf(1).get(0);
        for (NodeId n : c.groupNodes.get(1)) {
            c.crash(n);
        }
        c.await(admin.move(movingShard, 2), WAIT);
        c.run(Duration.ofSeconds(3));
        for (var r : c.replicas(GroupId.of(2))) {
            ShardKVStateMachine g2 = (ShardKVStateMachine) r.stateMachine();
            assertThat(g2.status(movingShard)).as("seed %d", seed).isEqualTo(ShardStatus.PULLING);
        }
        // A key on one of group 2's own shards is still served.
        String served = null;
        for (int i = 0; i < 40 && served == null; i++) {
            int s = Shards.shardOf(b("k" + i), 10);
            if (config.gidOf(s) == 2) {
                served = "k" + i;
            }
        }
        assertThat(c.await(client.getAsync(b(served)), Duration.ofSeconds(5)).isPresent()).isTrue();
        for (NodeId n : c.groupNodes.get(1)) {
            c.restart(n);
        }
        checkAll(c, client, "k", 40);
        c.awaitQuiescent(WAIT);
        c.checkSettledOwnership();
    }

    /** SPEC §6.7: the controller is down; data groups keep serving their current configuration. */
    @Test
    void dataGroupsServeWhileTheControllerIsDown() {
        ShardedCluster c = cluster(Seeds.resolve(), 2, false);
        ControllerClient admin = c.newAdmin(CLIENT);
        ShardedKVClient client = c.newClient(CLIENT);
        c.await(admin.join(c.groupsFor(1, 2)), WAIT);
        putAll(c, client, "k", 20); // the client now has the configuration cached
        for (NodeId n : c.controllerNodes) {
            c.crash(n);
        }
        putAll(c, client, "x", 20);
        checkAll(c, client, "k", 20);
        for (NodeId n : c.controllerNodes) {
            c.restart(n);
        }
        c.awaitQuiescent(WAIT);
    }

    /** SPEC §6.7: leaders crash, messages are duplicated, snapshots are taken while shards move. */
    @Test
    void migrationSurvivesCrashesDuplicatesAndSnapshots() {
        for (long seed = 1; seed <= 5; seed++) {
            ShardedCluster c = new ShardedCluster(seed, 3, 10, RaftConfig.defaults().withSnapshots(15, 200),
                    seed % 2 == 0 ? ShardedCluster.Storage.DISK : ShardedCluster.Storage.MEMORY, false);
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(10));
            c.startAll();
            ControllerClient admin = c.newAdmin(CLIENT);
            ShardedKVClient client = c.newClient(CLIENT);
            c.await(admin.join(c.groupsFor(1)), WAIT);
            putAll(c, client, "k", 50);
            c.net.setDuplicateRate(0.3);
            CompletableFuture<ShardConfig> join = admin.join(c.groupsFor(2, 3));
            for (int round = 0; round < 6; round++) {
                c.run(Duration.ofMillis(150));
                for (int gid = 1; gid <= 3; gid++) {
                    c.leader(GroupId.of(gid)).ifPresent(l -> {
                        c.crash(l.id());
                        c.run(Duration.ofMillis(50));
                        c.restart(l.id());
                    });
                }
            }
            c.await(join, WAIT);
            c.net.setDuplicateRate(0);
            checkAll(c, client, "k", 50);
            c.awaitQuiescent(WAIT);
            c.checkSettledOwnership();
        }
    }
}
