package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Milestone 5 gate: 300 KV chaos runs (half on simulated disks) where clients write through the
 * real client library; exactly-once must hold in every one. Run more with -Draftkv.seeds=N.
 */
class KVChaosTest {

    @Test
    void exactlyOnceHoldsUnderChaos() {
        List<Long> seeds = ElectionChaosTest.seedsToRun(Integer.getInteger("raftkv.seeds", 300));
        List<String> failures = new ArrayList<>();
        long ops = 0;
        long retries = 0;
        int i = 0;
        for (long seed : seeds) {
            SimulatedCluster.Storage storage = i++ % 2 == 0 ? SimulatedCluster.Storage.MEMORY : SimulatedCluster.Storage.DISK;
            KVChaos.Result r = KVChaos.run(seed, RaftConfig.defaults(), storage);
            r.failure().ifPresent(f -> failures.add("seed " + r.seed() + " (" + storage + "): " + f));
            ops += r.operations();
            retries += r.retries();
        }
        assertThat(failures).as("failing seeds:\n%s", String.join("\n", failures)).isEmpty();
        assertThat(ops).as("operations completed").isGreaterThan(seeds.size() * 30L);
        assertThat(retries).as("the clients really had to retry").isGreaterThan(seeds.size() * 2L);
    }
}
