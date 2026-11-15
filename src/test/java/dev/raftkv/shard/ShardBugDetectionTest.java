package dev.raftkv.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import dev.raftkv.raft.Bug;
import dev.raftkv.raft.RaftConfig;
import org.junit.jupiter.api.Test;

/**
 * The shard-migration bugs of SPEC §9.6 must each make the sharded chaos run fail, by breaking a
 * sharding invariant, losing data, or producing a non-linearizable history. An inconclusive
 * linearizability search does not count as catching anything.
 */
class ShardBugDetectionTest {

    private static final int BUDGET = 200;

    private static Optional<Integer> firstFailingSeed(Bug bug) {
        RaftConfig buggy = RaftConfig.defaults().withBugs(bug);
        for (int seed = 1; seed <= BUDGET; seed++) {
            ShardedChaos.Result r = ShardedChaos.run(seed, buggy, ShardedCluster.Storage.MEMORY);
            if (r.failure().isPresent() && !r.failure().get().startsWith("INCONCLUSIVE")) {
                return Optional.of(seed);
            }
        }
        return Optional.empty();
    }

    private static void check(Bug bug) {
        Optional<Integer> found = firstFailingSeed(bug);
        System.out.println("[bug-detection] " + bug + " (sharded chaos): "
                + found.map(s -> "caught at seed " + s).orElse("NOT caught in " + BUDGET + " seeds"));
        assertThat(found).isPresent();
    }

    @Test
    void detectsSessionsThatDoNotMoveWithTheirShard() {
        check(Bug.SESSIONS_NOT_MIGRATED);
    }

    @Test
    void detectsServingAFrozenShard() {
        check(Bug.SERVE_WHILE_BEPULLING);
    }

    @Test
    void detectsSkippedConfigurations() {
        check(Bug.SKIP_CONFIG_NUMBERS);
    }

    /**
     * Random chaos rarely times a leader change between the fetch and the commit of the insert, so
     * the scripted scenario is the reliable detector (like the Figure 8 test for Raft).
     */
    @Test
    void detectsGarbageCollectingBeforeTheInsertIsCommitted() {
        int caught = 0;
        for (long seed = 1; seed <= 10; seed++) {
            assertThat(GcScenario.run(seed, RaftConfig.defaults())).as("correct code, seed %d", seed).isEmpty();
            if (GcScenario.run(seed, RaftConfig.defaults().withBugs(Bug.GC_BEFORE_INSERT_COMMITTED)).isPresent()) {
                caught++;
            }
        }
        System.out.println("[bug-detection] GC_BEFORE_INSERT_COMMITTED: caught by a scripted scenario in " + caught + " of 10 seeds");
        assertThat(caught).isGreaterThanOrEqualTo(5);
    }

    @Test
    void detectsCheckingOwnershipOnlyOnArrival() {
        check(Bug.CHECK_OWNERSHIP_ON_RECEIVE_ONLY);
    }

    @Test
    void detectsDisabledDeduplicationAcrossGroups() {
        check(Bug.SESSION_DEDUP_DISABLED);
    }

    @Test
    void correctCodePassesTheSameSeeds() {
        for (int seed = 1; seed <= 30; seed++) {
            assertThat(ShardedChaos.run(seed, RaftConfig.defaults(), ShardedCluster.Storage.MEMORY).failure())
                    .as("seed %d", seed).isEmpty();
        }
    }
}
