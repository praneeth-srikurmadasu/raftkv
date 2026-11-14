package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import dev.raftkv.sim.SimulationException;
import org.junit.jupiter.api.Test;

/**
 * Milestone 3 bugs (SPEC §9.6): each must be caught by the replication harness within a bounded
 * number of fixed seeds. The Figure 8 bug also has its own scripted test in {@link Figure8Test}.
 */
class ReplicationBugDetectionTest {

    private static final int MAX_SEEDS = 300;

    /**
     * Caught deterministically by the scripted Figure 8 scenario. Random chaos has to stumble
     * into a Figure 8 shape by itself, which is rare and depends on traffic patterns, so its
     * result is only reported, not required (see docs/learning/03-log-replication.md §8).
     */
    @Test
    void detectsCommittingEarlierTermEntriesByCounting() {
        assertThatThrownBy(() -> Figure8Test.play(1, RaftConfig.defaults().withBugs(Bug.COMMIT_PREVIOUS_TERM_BY_COUNT)))
                .isInstanceOf(SimulationException.class);
        report(Bug.COMMIT_PREVIOUS_TERM_BY_COUNT, Optional.of(0));
    }

    /**
     * Random chaos cannot reach this bug in our implementation: the leader always sends the
     * pending entries with every AppendEntries, so a follower never holds "verified entries,
     * then a stale tail" when a higher leaderCommit arrives. The rule-level test can, so it
     * is the detector. (Implementations that send empty heartbeats while entries are pending
     * are exposed to this bug in practice; see docs/learning/03-log-replication.md.)
     */
    @Test
    void detectsCommitIndexBeyondTheVerifiedEntries() {
        FollowerAppendRulesTest t = new FollowerAppendRulesTest();
        t.startWith(RaftConfig.defaults().withBugs(Bug.COMMIT_INDEX_FROM_LAST_LOG_INDEX), 1,
                FollowerAppendRulesTest.e(1, 1), FollowerAppendRulesTest.e(2, 1), FollowerAppendRulesTest.e(3, 1));
        t.append(1, 1, 1, 3); // proves only index 1; leaderCommit is 3
        boolean caught = t.commitIndex() != 1;
        report(Bug.COMMIT_INDEX_FROM_LAST_LOG_INDEX, caught ? Optional.of(0) : Optional.empty());
        assertThat(caught).as("follower committed unverified entries").isTrue();
    }

    @Test
    void detectsTruncatingOnStaleAppends() {
        assertThat(seedsUntilChaosFails(Bug.TRUNCATE_ON_STALE_APPEND)).isPresent();
    }

    @Test
    void detectsVotingWithoutTheLogCheckThroughLostCommittedEntries() {
        // In Milestone 2 only a scripted scenario caught this; with real replication, chaos does too.
        assertThat(seedsUntilChaosFails(Bug.VOTE_WITHOUT_LOG_CHECK)).isPresent();
    }

    @Test
    void detectsAMissingNoopAfterElection() {
        Optional<Integer> found = Optional.empty();
        RaftConfig buggy = RaftConfig.defaults().withBugs(Bug.NO_NOOP_ON_ELECTION);
        for (int seed = 1; seed <= MAX_SEEDS && found.isEmpty(); seed++) {
            if (!ReplicationTest.earlierTermEntryGetsApplied(seed, buggy)) {
                found = Optional.of(seed);
            }
        }
        report(Bug.NO_NOOP_ON_ELECTION, found);
        assertThat(found).isPresent();
    }

    @Test
    void correctCodePassesTheSameSeeds() {
        for (int seed = 1; seed <= 50; seed++) {
            assertThat(ReplicationChaos.run(seed, RaftConfig.defaults()).failure()).as("seed %d", seed).isEmpty();
        }
    }

    private static Optional<Integer> seedsUntilChaosFails(Bug bug) {
        RaftConfig buggy = RaftConfig.defaults().withBugs(bug);
        for (int seed = 1; seed <= MAX_SEEDS; seed++) {
            if (!ReplicationChaos.run(seed, buggy).passed()) {
                report(bug, Optional.of(seed));
                return Optional.of(seed);
            }
        }
        report(bug, Optional.empty());
        return Optional.empty();
    }

    private static void report(Bug bug, Optional<Integer> seed) {
        System.out.println("[bug-detection] " + bug + ": "
                + seed.map(s -> s == 0 ? "caught by a scripted scenario / rule-level test" : "caught at seed " + s).orElse("NOT caught in " + MAX_SEEDS + " seeds"));
    }
}
