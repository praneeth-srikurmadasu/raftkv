package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * The Milestone 12 gate (SPEC §11): the group grows from 3 to 5 voters and shrinks back, over and
 * over, while clients run, nodes crash, partitions come and go and messages are lost; every client
 * history must stay linearizable and every Raft invariant must hold. 300 seeds per build
 * (alternating in-memory and on-disk storage); more with {@code -Draftkv.seeds=N}.
 */
class MembershipChaosTest {

    @Test
    void membershipChurnKeepsEveryHistoryLinearizable() {
        List<Long> seeds = ElectionChaosTest.seedsToRun(Integer.getInteger("raftkv.seeds", 300));
        List<String> failures = new ArrayList<>();
        long operations = 0;
        for (int i = 0; i < seeds.size(); i++) {
            long seed = seeds.get(i);
            SimulatedCluster.Storage storage = i % 2 == 0 ? SimulatedCluster.Storage.MEMORY : SimulatedCluster.Storage.DISK;
            LinearizabilityChaos.Result r = LinearizabilityChaos.run(seed, RaftConfig.defaults(), storage, true);
            r.failure().ifPresent(f -> failures.add("seed " + seed + " (" + storage + "): " + f));
            operations += r.operations();
        }
        System.out.println("[membership] " + seeds.size() + " runs, " + operations + " operations: "
                + (failures.isEmpty() ? "all linearizable" : failures.size() + " FAILED"));
        assertThat(failures).as("failing seeds (replay with -Draftkv.seed=<seed>):\n%s", String.join("\n", failures)).isEmpty();
    }
}
