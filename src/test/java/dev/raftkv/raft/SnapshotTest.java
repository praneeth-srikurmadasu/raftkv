package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/**
 * Milestone 8: snapshots, log compaction and InstallSnapshot (Raft §7, Figure 13).
 */
class SnapshotTest {

    /** Snapshot every 20 entries, in tiny chunks, so every test sends many chunks. */
    private static final RaftConfig CONFIG = RaftConfig.defaults().withSnapshots(20, 64);

    private static List<String> applied(SimulatedCluster c, NodeId id) {
        return c.stateMachine(id).applied().stream().map(a -> a.command().toUtf8()).toList();
    }

    private static void writeAll(SimulatedCluster c, String prefix, int n) {
        List<CompletableFuture<CommitResult>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            List<NodeId> up = c.ids.stream().filter(c::isRunning).toList();
            RaftNode leader = c.awaitLeader(up, Duration.ofSeconds(10));
            futures.add(leader.propose(Bytes.ofUtf8(prefix + i)));
            c.run(Duration.ofMillis(2));
        }
        c.sim.runUntil(() -> futures.stream().allMatch(CompletableFuture::isDone), Duration.ofSeconds(30));
    }

    @Test
    void everyNodeSnapshotsAndCompactsItsLog() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        writeAll(c, "w", 200);
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(5));
        c.awaitAppliedEverywhere(leader.commitIndex(), Duration.ofSeconds(30));
        for (NodeId id : c.ids) {
            RaftNode n = c.node(id);
            // How many snapshots a follower takes depends on how its commit index advances (with
            // pipelining it may learn of 200 commits at once and snapshot once); what matters is
            // that it snapshots and its log stays short.
            assertThat(n.snapshotsTaken() + n.snapshotsInstalled()).as("seed %d %s", seed, id).isGreaterThan(0L);
            assertThat(c.log(id).snapshotIndex()).as("seed %d %s", seed, id).isGreaterThan(150L);
            assertThat(c.log(id).lastIndex() - c.log(id).snapshotIndex()).as("log stays short").isLessThan(60L);
            assertThat(applied(c, id)).as("seed %d", seed).isEqualTo(applied(c, leader.id()));
        }
    }

    /**
     * The Milestone 8 gate, part 1: a follower that missed so much that the leader no longer has
     * the entries it needs catches up through InstallSnapshot, then continues from the log.
     */
    @Test
    void aLaggingFollowerCatchesUpThroughASnapshot() {
        for (long seed = 1; seed <= 20; seed++) {
            SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
            c.startAll();
            RaftNode leader = c.awaitLeader(Duration.ofSeconds(5));
            NodeId lagging = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            c.crash(lagging);
            writeAll(c, "while-down-", 150);
            RaftNode now = c.awaitLeader(c.ids.stream().filter(id -> !id.equals(lagging)).toList(), Duration.ofSeconds(5));
            assertThat(c.log(now.id()).snapshotIndex()).as("seed %d: leader compacted past the follower", seed)
                    .isGreaterThan(c.log(lagging).lastIndex());

            c.restart(lagging);
            writeAll(c, "after-", 30);
            RaftNode l = c.awaitLeader(Duration.ofSeconds(5));
            c.awaitAppliedEverywhere(l.commitIndex(), Duration.ofSeconds(30));
            assertThat(c.node(lagging).snapshotsInstalled()).as("seed %d", seed).isGreaterThanOrEqualTo(1L);
            assertThat(applied(c, lagging)).as("seed %d", seed).isEqualTo(applied(c, l.id()));
            assertThat(applied(c, lagging).size()).isEqualTo(180);
        }
    }

    /** Chunks get lost and duplicated; the transfer resumes from where the follower got to. */
    @Test
    void installSnapshotSurvivesALossyNetwork() {
        for (long seed = 1; seed <= 20; seed++) {
            SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG.withSnapshots(20, 7));
            c.startAll();
            RaftNode leader = c.awaitLeader(Duration.ofSeconds(5));
            NodeId lagging = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            c.crash(lagging);
            writeAll(c, "x", 100);
            c.net.setDropRate(0.2);
            c.net.setDuplicateRate(0.2);
            c.restart(lagging);
            c.run(Duration.ofSeconds(10));
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            RaftNode l = c.awaitLeader(Duration.ofSeconds(10));
            CompletableFuture<CommitResult> marker = l.propose(Bytes.ofUtf8("marker"));
            c.sim.runUntil(marker::isDone, Duration.ofSeconds(10));
            c.awaitAppliedEverywhere(marker.join().index(), Duration.ofSeconds(60));
            assertThat(applied(c, lagging)).as("seed %d", seed).isEqualTo(applied(c, c.awaitLeader(Duration.ofSeconds(5)).id()));
        }
    }

    /** The Milestone 8 gate, part 2: every node restarts from its snapshot plus the log after it. */
    @Test
    void nodesRestartFromTheirSnapshotOnDisk() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.withDisks(seed, 3, CONFIG);
        c.startAll();
        writeAll(c, "d", 120);
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(5));
        c.awaitAppliedEverywhere(leader.commitIndex(), Duration.ofSeconds(30));
        List<String> before = applied(c, leader.id());
        for (NodeId id : c.ids) {
            c.crash(id);
        }
        for (NodeId id : c.ids) {
            c.restart(id);
            RaftNode n = c.node(id);
            long snap = n.latestSnapshot().orElseThrow().lastIncludedIndex();
            assertThat(n.lastApplied()).as("seed %d: restored from the snapshot, not from index 0", seed).isEqualTo(snap);
            assertThat(c.log(id).snapshotIndex()).isEqualTo(snap);
        }
        writeAll(c, "e", 10);
        RaftNode l = c.awaitLeader(Duration.ofSeconds(5));
        c.awaitAppliedEverywhere(l.commitIndex(), Duration.ofSeconds(30));
        for (NodeId id : c.ids) {
            List<String> after = applied(c, id);
            assertThat(after.subList(0, before.size())).as("seed %d %s", seed, id).isEqualTo(before);
            assertThat(after.size()).isEqualTo(130);
        }
    }

    /** All nodes lose power at once (unsynced writes lost) while snapshots are being taken. */
    @Test
    void allNodesCrashWithSnapshotsOnAndNothingAcknowledgedIsLost() {
        for (long seed = 1; seed <= 20; seed++) {
            assertThat(PersistenceTest.allNodesCrashScenario(seed, CONFIG)).as("seed %d", seed).isEmpty();
        }
    }

    /** The KV store keeps exactly-once semantics across snapshot installs. */
    @Test
    void kvStoreIsExactlyOnceAcrossSnapshots() {
        for (long seed = 1; seed <= 10; seed++) {
            SimulatedCluster c = SimulatedCluster.kv(seed, 3, CONFIG, SimulatedCluster.Storage.MEMORY);
            c.startAll();
            c.awaitLeader(Duration.ofSeconds(5));
            RaftKVClient client = c.newClient(ClientConfig.defaults());
            NodeId victim = NodeId.of(1 + (int) (seed % 3));
            c.crash(victim);
            List<CompletableFuture<?>> ops = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                ops.add(client.putAsync(Bytes.ofUtf8("k" + (i % 10)), Bytes.ofUtf8("v" + i)));
            }
            c.sim.runUntil(() -> ops.stream().allMatch(CompletableFuture::isDone), Duration.ofSeconds(60));
            c.restart(victim);
            c.net.setDuplicateRate(0.3);
            for (int i = 0; i < 30; i++) {
                c.await(client.putAsync(Bytes.ofUtf8("late" + i), Bytes.ofUtf8("x")), Duration.ofSeconds(30));
            }
            c.net.setDuplicateRate(0);
            c.awaitIdenticalApplied(Duration.ofSeconds(30));
            long digest = c.kvStore(c.ids.get(0)).digest();
            for (NodeId id : c.ids) {
                assertThat(c.kvStore(id).executedCommands()).as("seed %d %s", seed, id).isEqualTo(130L);
                assertThat(c.kvStore(id).digest()).as("seed %d %s", seed, id).isEqualTo(digest);
            }
            assertThat(c.node(victim).snapshotsInstalled()).as("seed %d", seed).isGreaterThanOrEqualTo(1L);
            assertThat(c.kvStore(victim).peek(Bytes.ofUtf8("k9"))).isEqualTo(c.kvStore(c.ids.get(0)).peek(Bytes.ofUtf8("k9")));
            assertThat(c.kvStore(victim).size()).isEqualTo(40);
        }
    }

    @Test
    void snapshotsAreOffWhenTheThresholdIsZero() {
        SimulatedCluster c = new SimulatedCluster(Seeds.resolve(), 3, RaftConfig.defaults().withSnapshots(0, 64));
        c.startAll();
        writeAll(c, "n", 100);
        for (RaftNode n : c.runningNodes()) {
            assertThat(n.snapshotsTaken()).isEqualTo(0L);
            assertThat(c.log(n.id()).snapshotIndex()).isEqualTo(0L);
        }
    }

    /**
     * Regression: chunks are pipelined (a window in flight), and a reply that teaches nothing new
     * must not trigger more sends. Otherwise duplicated replies multiply into ever more chunks
     * (an earlier version ran out of memory this way under 20% duplication).
     */
    @Test
    void snapshotTransferDoesNotMultiplyMessages() {
        for (long seed = 1; seed <= 10; seed++) {
            SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG.withSnapshots(20, 16));
            int[] chunks = {0};
            c.deliveryObserver = e -> {
                if (e.message() instanceof dev.raftkv.protocol.InstallSnapshot) {
                    chunks[0]++;
                }
            };
            c.startAll();
            RaftNode leader = c.awaitLeader(Duration.ofSeconds(5));
            NodeId lagging = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
            c.crash(lagging);
            writeAll(c, "x", 100);
            c.net.setDuplicateRate(0.3);
            c.net.setDropRate(0.05);
            c.restart(lagging);
            c.sim.runUntil(() -> c.node(lagging).snapshotsInstalled() > 0, Duration.ofSeconds(60));
            assertThat(c.node(lagging).snapshotsInstalled()).as("seed %d", seed).isGreaterThan(0L);
            int needed = c.node(lagging).latestSnapshot().orElseThrow().data().length() / 16 + 1;
            assertThat(chunks[0]).as("seed %d: %d chunk deliveries for %d chunks", seed, chunks[0], needed)
                    .isLessThan(needed * 10);
        }
    }
}
