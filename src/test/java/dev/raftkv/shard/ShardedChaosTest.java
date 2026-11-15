package dev.raftkv.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import dev.raftkv.raft.RaftConfig;
import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/**
 * The Milestone 10 gate (SPEC §11): sharded swarm runs with concurrent Join/Leave/Move stay
 * linearizable and keep every sharding invariant. 2,000 seeds per build (alternating in-memory
 * and on-disk storage); run more with {@code -Draftkv.seeds=N}, replay one with {@code -Draftkv.seed=S}.
 */
class ShardedChaosTest {

    @Test
    void shardedHistoriesAreLinearizableWhileShardsMove() {
        List<Long> seeds = seedsToRun(Integer.getInteger("raftkv.seeds", 2000));
        List<String> failures = new ArrayList<>();
        long ops = 0;
        long configs = 0;
        for (int i = 0; i < seeds.size(); i++) {
            long seed = seeds.get(i);
            ShardedCluster.Storage storage = i % 2 == 0 ? ShardedCluster.Storage.MEMORY : ShardedCluster.Storage.DISK;
            ShardedChaos.Result r = ShardedChaos.run(seed, RaftConfig.defaults(), storage);
            r.failure().ifPresent(f -> failures.add("seed " + r.seed() + " (" + storage + "):\n" + f));
            ops += r.operations();
            configs += r.configs();
        }
        assertThat(failures).as("failed sharded runs (replay with -Draftkv.seed=<seed>):\n%s", String.join("\n", failures))
                .isEmpty();
        System.out.println("[sharded] " + seeds.size() + " runs, " + ops + " operations, " + configs
                + " reconfigurations: all linearizable, invariants held");
    }

    static List<Long> seedsToRun(int count) {
        if (Seeds.isFixed()) {
            return List.of(Seeds.resolve());
        }
        java.util.Random base = new java.util.Random(Seeds.resolve());
        List<Long> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(base.nextLong());
        }
        return out;
    }
}
