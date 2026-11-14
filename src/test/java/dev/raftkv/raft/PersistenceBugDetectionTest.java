package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

/** Milestone 4 bug (SPEC §9.6): acknowledging before fsync must be caught. */
class PersistenceBugDetectionTest {

    private static final int MAX_SEEDS = 200;

    @Test
    void detectsAcknowledgingEntriesBeforeTheyAreOnDisk() {
        RaftConfig buggy = RaftConfig.defaults().withBugs(Bug.REPLY_BEFORE_FSYNC);
        Optional<Integer> found = Optional.empty();
        for (int seed = 1; seed <= MAX_SEEDS && found.isEmpty(); seed++) {
            if (!ReplicationChaos.run(seed, buggy, SimulatedCluster.Storage.DISK).passed()) {
                found = Optional.of(seed);
            }
        }
        System.out.println("[bug-detection] REPLY_BEFORE_FSYNC (disk chaos): "
                + found.map(s -> "caught at seed " + s).orElse("NOT caught in " + MAX_SEEDS + " seeds"));
        assertThat(found).isPresent();
    }

    @Test
    void theAllNodesCrashScenarioAlsoCatchesIt() {
        RaftConfig buggy = RaftConfig.defaults().withBugs(Bug.REPLY_BEFORE_FSYNC);
        Optional<Integer> found = Optional.empty();
        for (int seed = 1; seed <= 50 && found.isEmpty(); seed++) {
            Optional<String> lost;
            try {
                lost = PersistenceTest.allNodesCrashScenario(seed, buggy);
            } catch (AssertionError | RuntimeException e) {
                lost = Optional.of(String.valueOf(e.getMessage())); // an invariant fired first
            }
            if (lost.isPresent()) {
                found = Optional.of(seed);
            }
        }
        System.out.println("[bug-detection] REPLY_BEFORE_FSYNC (all-nodes crash): "
                + found.map(s -> "caught at seed " + s).orElse("NOT caught"));
        assertThat(found).isPresent();
    }

    @Test
    void correctCodePassesTheSameSeeds() {
        for (int seed = 1; seed <= 30; seed++) {
            assertThat(ReplicationChaos.run(seed, RaftConfig.defaults(), SimulatedCluster.Storage.DISK).failure())
                    .as("seed %d", seed).isEmpty();
        }
    }
}
