package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Milestone 3 gate (SPEC §11): replication stays safe across 1,000 randomized chaos runs with a
 * client writing throughout. Run more with {@code -Draftkv.seeds=10000}; replay one with
 * {@code -Draftkv.seed=<seed>}.
 */
class ReplicationChaosTest {

    @Test
    void replicationIsSafeAndLiveUnderChaos() {
        List<Long> seeds = ElectionChaosTest.seedsToRun(Integer.getInteger("raftkv.seeds", 1000));
        List<String> failures = new ArrayList<>();
        long committed = 0;
        long acknowledged = 0;
        for (long seed : seeds) {
            ReplicationChaos.Result r = ReplicationChaos.run(seed, RaftConfig.defaults());
            r.failure().ifPresent(f -> failures.add("seed " + r.seed() + " (" + r.size() + " nodes): " + f));
            committed += r.committed();
            acknowledged += r.acknowledged();
        }
        assertThat(failures).as("failing seeds (replay with -Draftkv.seed=<seed>):\n%s", String.join("\n", failures)).isEmpty();
        assertThat(acknowledged).as("the client really got writes through").isGreaterThan(seeds.size() * 50L);
        assertThat(committed).isGreaterThan(acknowledged);
    }
}
