package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Milestone 6 gate (SPEC §11): 1,000 swarm-chaos runs, half on simulated disks, every client
 * operation recorded and the full history checked for linearizability. Replay a failure with
 * {@code -Draftkv.seed=<seed>}; run more with {@code -Draftkv.seeds=N}.
 */
class LinearizabilityChaosTest {

    @Test
    void everyHistoryIsLinearizable() {
        List<Long> seeds = ElectionChaosTest.seedsToRun(Integer.getInteger("raftkv.seeds", 1000));
        List<String> failures = new ArrayList<>();
        long ops = 0;
        long unknown = 0;
        int i = 0;
        for (long seed : seeds) {
            SimulatedCluster.Storage storage = i++ % 2 == 0 ? SimulatedCluster.Storage.MEMORY : SimulatedCluster.Storage.DISK;
            LinearizabilityChaos.Result r = LinearizabilityChaos.run(seed, RaftConfig.defaults(), storage);
            r.failure().ifPresent(f -> failures.add("seed " + r.seed() + " (" + storage + "):\n" + f));
            ops += r.operations();
            unknown += r.unknown();
        }
        assertThat(failures).as("non-linearizable runs (replay with -Draftkv.seed=<seed>):\n%s",
                String.join("\n", failures.subList(0, Math.min(3, failures.size())))).isEmpty();
        assertThat(ops).as("operations checked").isGreaterThan(seeds.size() * 100L);
        if (seeds.size() >= 100) {
            assertThat(unknown).as("some operations had unknown outcomes (that path must be exercised)").isGreaterThan(0L);
        }
        System.out.println("[linearizability] " + seeds.size() + " runs, " + ops + " operations, " + unknown + " with unknown outcome: all linearizable");
    }
}
