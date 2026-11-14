package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * The linearizability checker as a bug detector (SPEC §9.6): bugs that only show up as wrong
 * answers to clients, never as a broken Raft invariant, must still be caught.
 */
class LinearizabilityBugDetectionTest {

    private static Optional<Integer> firstFailingSeed(Bug bug, int maxSeeds) {
        RaftConfig buggy = RaftConfig.defaults().withBugs(bug);
        for (int seed = 1; seed <= maxSeeds; seed++) {
            // Only a PROVEN violation counts; an inconclusive search does not.
            if (LinearizabilityChaos.run(seed, buggy, SimulatedCluster.Storage.MEMORY).definitelyNotLinearizable()) {
                return Optional.of(seed);
            }
        }
        return Optional.empty();
    }

    private static void report(Bug bug, Optional<Integer> seed, int max) {
        System.out.println("[bug-detection] " + bug + " (linearizability): "
                + seed.map(s -> "caught at seed " + s).orElse("NOT caught in " + max + " seeds"));
    }

    /** Stale reads keep every Raft invariant intact; only the clients' view proves them wrong. */
    @Test
    void detectsStaleReadsFromLocalState() {
        Optional<Integer> found = firstFailingSeed(Bug.READ_FROM_LOCAL_STATE, 100);
        report(Bug.READ_FROM_LOCAL_STATE, found, 100);
        assertThat(found).isPresent();
    }

    @Test
    void detectsRetriesThatExecuteTwice() {
        Optional<Integer> found = firstFailingSeed(Bug.SESSION_DEDUP_DISABLED, 100);
        report(Bug.SESSION_DEDUP_DISABLED, found, 100);
        assertThat(found).isPresent();
    }

    @Test
    void correctCodePassesTheSameSeeds() {
        for (int seed = 1; seed <= 30; seed++) {
            assertThat(LinearizabilityChaos.run(seed, RaftConfig.defaults(), SimulatedCluster.Storage.MEMORY).failure())
                    .as("seed %d", seed).isEmpty();
        }
    }
}
