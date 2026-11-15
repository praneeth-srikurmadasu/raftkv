package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/**
 * Raft on the real on-disk storage (FileMetaStore + SegmentedFileLog), on simulated disks that
 * lose or tear everything not fsynced when a node crashes.
 */
class PersistenceTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();

    /**
     * The Milestone 4 gate (SPEC §11): every node of the cluster loses power at the same moment,
     * with writes still in flight. After the restart, every write that had been acknowledged
     * is still there, at the same index, and every replica applies it.
     */
    @Test
    void allNodesCrashAtOnceAndEveryAcknowledgedWriteSurvives() {
        for (long seed = 1; seed <= 20; seed++) {
            assertThat(allNodesCrashScenario(seed, CONFIG)).as("seed %d", seed).isEmpty();
        }
    }

    @Test
    void allNodesCrashWithARandomSeed() {
        long seed = Seeds.resolve();
        assertThat(allNodesCrashScenario(seed, CONFIG)).as("seed %d", seed).isEmpty();
    }

    /** Runs the scenario; returns a description of the first lost acknowledged write, if any. */
    static Optional<String> allNodesCrashScenario(long seed, RaftConfig config) {
        SimulatedCluster c = SimulatedCluster.withDisks(seed, seed % 2 == 0 ? 3 : 5, config);
        c.net.setLatency(java.time.Duration.ofMillis(1), java.time.Duration.ofMillis(15));
        c.startAll();
        Map<Long, String> acknowledged = new TreeMap<>();
        for (int round = 0; round < 3; round++) {
            c.awaitLeader(Duration.ofSeconds(5));
            for (int i = 0; i < 60; i++) {
                String cmd = "r" + round + "-" + i;
                c.propose(cmd).thenAccept(res -> acknowledged.put(res.index(), cmd));
                c.run(Duration.ofMillis(5));          // writes keep flowing ...
            }
            for (NodeId id : c.ids) {                 // ... when every node loses power at once
                c.crash(id);
            }
            c.run(Duration.ofMillis(200));
            for (NodeId id : c.ids) {
                c.restart(id);
            }
        }
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(5));
        CompletableFuture<CommitResult> marker = leader.propose(Bytes.ofUtf8("marker"));
        c.sim.runUntil(marker::isDone, Duration.ofSeconds(5));
        c.awaitAppliedEverywhere(marker.join().index(), Duration.ofSeconds(30));
        // Check against the leader's applied history (which survives snapshots), not its log.
        Map<Long, Bytes> appliedAt = new TreeMap<>();
        for (RecordingStateMachine.Applied a : c.stateMachine(leader.id()).applied()) {
            appliedAt.put(a.index(), a.command());
        }
        for (Map.Entry<Long, String> ack : acknowledged.entrySet()) {
            Bytes there = appliedAt.get(ack.getKey());
            if (there == null || !there.equals(Bytes.ofUtf8(ack.getValue()))) {
                return Optional.of("acknowledged " + ack.getValue() + " at " + ack.getKey() + " lost; applied there: "
                        + (there == null ? "nothing" : there.toUtf8()));
            }
        }
        if (acknowledged.size() < 100) {
            return Optional.of("too few writes were acknowledged to make the test meaningful: " + acknowledged.size());
        }
        return Optional.empty();
    }

    @Test
    void restartedNodeRemembersItsTermAndVoteFromDisk() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.withDisks(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(3));
        NodeId follower = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        long term = c.node(follower).currentTerm();
        Optional<NodeId> vote = c.node(follower).votedFor();
        c.crash(follower);
        c.restart(follower);
        assertThat(c.node(follower).currentTerm()).as("seed %d", seed).isEqualTo(term);
        assertThat(c.node(follower).votedFor()).as("seed %d", seed).isEqualTo(vote);
    }

    /** Group commit: many proposals in the same instant share ONE fsync of the leader's log. */
    @Test
    void manyProposalsShareOneFsync() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.withDisks(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(3));
        c.run(Duration.ofMillis(500)); // let the no-op settle
        long syncsBefore = leader.logSyncs();
        List<CompletableFuture<CommitResult>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(leader.propose(Bytes.ofUtf8("batch-" + i)));
        }
        c.sim.runUntil(() -> leader.logSyncs() > syncsBefore, Duration.ofSeconds(1));
        assertThat(leader.logSyncs() - syncsBefore).as("seed %d: one fsync for all 100 appends", seed).isEqualTo(1L);
        c.sim.runUntil(() -> futures.stream().allMatch(CompletableFuture::isDone), Duration.ofSeconds(3));
        assertThat(futures.stream().noneMatch(CompletableFuture::isCompletedExceptionally)).isTrue();
    }

    @Test
    void crashedFollowerRecoversItsLogFromDiskAndCatchesUp() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.withDisks(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(3));
        CompletableFuture<CommitResult> last = null;
        for (int i = 0; i < 200; i++) {
            last = leader.propose(Bytes.ofUtf8("w" + i));
        }
        CompletableFuture<CommitResult> lastWrite = last;
        c.sim.runUntil(lastWrite::isDone, Duration.ofSeconds(5));
        NodeId follower = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        c.awaitAppliedEverywhere(lastWrite.join().index(), Duration.ofSeconds(5));
        long followerLast = c.log(follower).lastIndex();
        c.crash(follower);
        c.restart(follower);
        // Everything it acknowledged was fsynced, so recovery brings back at least that much.
        assertThat(c.log(follower).lastIndex()).as("seed %d", seed).isEqualTo(followerLast);
        c.awaitAppliedEverywhere(lastWrite.join().index(), Duration.ofSeconds(5));
        assertThat(c.stateMachine(follower).applied()).hasSize(200);
    }
}
