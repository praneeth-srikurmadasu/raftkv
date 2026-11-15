package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import org.junit.jupiter.api.Test;

/**
 * Proves the election tests have teeth (SPEC §9.6): with each deliberate bug switched on, the
 * same harness that passes on correct code must report a failure within a bounded number of
 * seeds. Seeds here are fixed (1, 2, 3, ...) so the measured "seeds to detection" are stable.
 */
class ElectionBugDetectionTest {

    private static final int MAX_SEEDS = 200;

    @Test
    void detectsVotingTwiceInATerm() {
        assertThat(seedsUntilChaosFails(Bug.VOTE_TWICE_IN_TERM)).isPresent();
    }

    @Test
    void detectsForgettingTheVoteAcrossARestart() {
        // Double votes need two real elections in the same term around a restart; pre-vote makes
        // real elections rare enough to hide this almost completely, so test the classic path.
        assertThat(seedsUntilChaosFails(Bug.VOTEDFOR_NOT_PERSISTED, false)).isPresent();
    }

    @Test
    void detectsALeaderThatIgnoresHigherTerms() {
        // Pre-vote keeps terms from racing ahead, which hides this bug almost completely (a good
        // thing in production); the classic election path must still be guarded, so test it there.
        assertThat(seedsUntilChaosFails(Bug.LEADER_IGNORES_HIGHER_TERM, false)).isPresent();
    }

    @Test
    void detectsVotingWithoutTheLogCheck() {
        RaftConfig buggy = RaftConfig.defaults().withBugs(Bug.VOTE_WITHOUT_LOG_CHECK);
        Optional<Integer> found = Optional.empty();
        for (int seed = 1; seed <= MAX_SEEDS && found.isEmpty(); seed++) {
            if (ElectionTest.staleLogScenarioWinner(seed, buggy).equals(NodeId.of(1))) {
                found = Optional.of(seed);
            }
        }
        report(Bug.VOTE_WITHOUT_LOG_CHECK, found);
        assertThat(found).isPresent();
    }

    /**
     * Liveness (SPEC §9.6): only an up-to-date node can win, and the stale ones keep campaigning.
     * If a rejected (pre-)vote restarts the voter's election timer, the only node that could win
     * keeps being reset by candidates that can't. Correct code elects it within a second every
     * time; with the bug the group stays leaderless far longer.
     */
    @Test
    void detectsResettingTheTimerOnARejectedVote() {
        RaftConfig correct = RaftConfig.defaults();
        RaftConfig buggy = correct.withBugs(Bug.RESET_TIMER_ON_REJECTED_VOTE);
        for (int seed = 1; seed <= 30; seed++) {
            assertThat(electsALeaderWithOnlyOneElectableNode(seed, correct)).as("correct code, seed %d", seed).isTrue();
        }
        Optional<Integer> found = Optional.empty();
        for (int seed = 1; seed <= 30 && found.isEmpty(); seed++) {
            if (!electsALeaderWithOnlyOneElectableNode(seed, buggy)) {
                found = Optional.of(seed);
            }
        }
        report(Bug.RESET_TIMER_ON_REJECTED_VOTE, found);
        assertThat(found).isPresent();
    }

    /**
     * 7 nodes; writes reach only 4 of them (a majority); 3 of those 4 crash. The survivor is the
     * only node that can win (the other 3 lack committed entries), and those 3 keep campaigning.
     */
    static boolean electsALeaderWithOnlyOneElectableNode(long seed, RaftConfig config) {
        SimulatedCluster c = new SimulatedCluster(seed, 7, config);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        List<NodeId> others = c.ids.stream().filter(id -> !id.equals(leader.id())).toList();
        NodeId fresh = others.get(0);
        List<NodeId> alsoFresh = others.subList(1, 3);
        List<NodeId> stale = others.subList(3, 6);
        for (NodeId s : stale) {
            c.net.blockOneWay(leader.id(), s);
        }
        CompletableFuture<CommitResult> last = null;
        for (int i = 0; i < 5; i++) {
            last = leader.propose(Bytes.ofUtf8("only-on-four-" + i));
        }
        CompletableFuture<CommitResult> lastWrite = last;
        c.sim.runUntil(lastWrite::isDone, Duration.ofSeconds(2));
        c.sim.runUntil(() -> c.node(fresh).log().lastIndex() >= lastWrite.join().index(), Duration.ofSeconds(2));
        c.crash(leader.id());
        alsoFresh.forEach(c::crash);
        c.net.heal();
        List<NodeId> alive = new java.util.ArrayList<>(stale);
        alive.add(fresh);
        return c.sim.runUntil(() -> alive.stream().anyMatch(id -> c.node(id).role() == Role.LEADER), Duration.ofSeconds(1));
    }

    @Test
    void correctCodePassesTheSameSeeds() {
        // The control experiment: without bugs, the very same seeds all pass.
        for (int seed = 1; seed <= 50; seed++) {
            ElectionChaos.Result r = ElectionChaos.run(seed, RaftConfig.defaults());
            assertThat(r.failure()).as("seed %d", seed).isEmpty();
        }
    }

    private static Optional<Integer> seedsUntilChaosFails(Bug bug) {
        return seedsUntilChaosFails(bug, true);
    }

    private static Optional<Integer> seedsUntilChaosFails(Bug bug, boolean preVote) {
        RaftConfig buggy = RaftConfig.defaults().withElectionExtensions(preVote, true).withBugs(bug);
        for (int seed = 1; seed <= MAX_SEEDS; seed++) {
            if (!ElectionChaos.run(seed, buggy).passed()) {
                report(bug, Optional.of(seed));
                return Optional.of(seed);
            }
        }
        report(bug, Optional.empty());
        return Optional.empty();
    }

    private static void report(Bug bug, Optional<Integer> seed) {
        System.out.println("[bug-detection] " + bug + ": "
                + seed.map(s -> "caught at seed " + s).orElse("NOT caught in " + MAX_SEEDS + " seeds"));
    }

}
