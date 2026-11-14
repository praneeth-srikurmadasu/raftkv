package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

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
        assertThat(seedsUntilChaosFails(Bug.VOTEDFOR_NOT_PERSISTED)).isPresent();
    }

    @Test
    void detectsALeaderThatIgnoresHigherTerms() {
        assertThat(seedsUntilChaosFails(Bug.LEADER_IGNORES_HIGHER_TERM)).isPresent();
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

    @Test
    void correctCodePassesTheSameSeeds() {
        // The control experiment: without bugs, the very same seeds all pass.
        for (int seed = 1; seed <= 50; seed++) {
            ElectionChaos.Result r = ElectionChaos.run(seed, RaftConfig.defaults());
            assertThat(r.failure()).as("seed %d", seed).isEmpty();
        }
    }

    private static Optional<Integer> seedsUntilChaosFails(Bug bug) {
        RaftConfig buggy = RaftConfig.defaults().withBugs(bug);
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
