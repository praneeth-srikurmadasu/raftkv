package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Replication chaos on real on-disk storage: every crash also loses or tears whatever the
 * crashed node had not fsynced. Same checks as {@link ReplicationChaosTest}, including "every
 * acknowledged write survives". Run more with {@code -Draftkv.seeds=5000}.
 */
class PersistenceChaosTest {

    @Test
    void replicationOnDisksSurvivesCrashesThatLoseUnsyncedData() {
        List<Long> seeds = ElectionChaosTest.seedsToRun(Integer.getInteger("raftkv.seeds", 500));
        List<String> failures = new ArrayList<>();
        long acknowledged = 0;
        for (long seed : seeds) {
            ReplicationChaos.Result r = ReplicationChaos.run(seed, RaftConfig.defaults(), SimulatedCluster.Storage.DISK);
            r.failure().ifPresent(f -> failures.add("seed " + r.seed() + " (" + r.size() + " nodes): " + f));
            acknowledged += r.acknowledged();
        }
        assertThat(failures).as("failing seeds (replay with -Draftkv.seed=<seed>):\n%s", String.join("\n", failures)).isEmpty();
        assertThat(acknowledged).isGreaterThan(seeds.size() * 50L);
    }
}
