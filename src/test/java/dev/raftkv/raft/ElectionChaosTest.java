package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/**
 * Milestone 2 gate (SPEC §11): leader election stays safe and live across 500 randomized
 * chaos runs with crashes, partitions, drops, duplicates and reordering.
 *
 * <p>Run more seeds locally with {@code -Draftkv.seeds=5000}; replay one failure with
 * {@code -Draftkv.seed=<seed>} (runs only that seed).
 */
class ElectionChaosTest {

    @Test
    void electionsAreSafeAndLiveUnderChaos() {
        List<Long> seeds = seedsToRun(Integer.getInteger("raftkv.seeds", 500));
        List<String> failures = new ArrayList<>();
        long totalLeaders = 0;
        for (long seed : seeds) {
            ElectionChaos.Result r = ElectionChaos.run(seed, RaftConfig.defaults());
            r.failure().ifPresent(f -> failures.add("seed " + r.seed() + " (" + r.size() + " nodes): " + f));
            totalLeaders += r.termsWithLeader();
        }
        assertThat(failures).as("failing seeds (replay one with -Draftkv.seed=<seed>):\n%s",
                String.join("\n", failures)).isEmpty();
        assertThat(totalLeaders).as("the chaos really forced many elections").isGreaterThan(seeds.size() * 3L);
    }

    /** One fixed seed from -Draftkv.seed, or {@code count} seeds derived from a random base. */
    static List<Long> seedsToRun(int count) {
        if (Seeds.isFixed()) {
            return List.of(Seeds.resolve());
        }
        Random base = new Random(Seeds.resolve());
        List<Long> seeds = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            seeds.add(base.nextLong());
        }
        return seeds;
    }
}
