package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

/** Milestone 5 bug (SPEC §9.6): turning off session deduplication must be caught. */
class KVBugDetectionTest {

    @Test
    void detectsDisabledSessionDeduplication() {
        RaftConfig buggy = RaftConfig.defaults().withBugs(Bug.SESSION_DEDUP_DISABLED);
        Optional<Integer> found = Optional.empty();
        for (int seed = 1; seed <= 100 && found.isEmpty(); seed++) {
            if (KVClusterTest.exactlyOnceScenario(seed, buggy).isPresent()
                    || !KVChaos.run(seed, buggy, SimulatedCluster.Storage.MEMORY).passed()) {
                found = Optional.of(seed);
            }
        }
        System.out.println("[bug-detection] SESSION_DEDUP_DISABLED: "
                + found.map(s -> "caught at seed " + s).orElse("NOT caught in 100 seeds"));
        assertThat(found).isPresent();
    }

    @Test
    void correctCodePassesTheSameSeeds() {
        for (int seed = 1; seed <= 20; seed++) {
            assertThat(KVClusterTest.exactlyOnceScenario(seed, RaftConfig.defaults())).as("seed %d", seed).isEmpty();
            assertThat(KVChaos.run(seed, RaftConfig.defaults(), SimulatedCluster.Storage.MEMORY).failure()).as("seed %d", seed).isEmpty();
        }
    }
}
