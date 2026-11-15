package dev.raftkv.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

/** The rebalancer's four properties (SPEC §6.3): balanced, minimal moves, deterministic, no shard on a departed group. */
class RebalancerTest {

    @Test
    void firstJoinTakesEverything() {
        int[] result = Rebalancer.rebalance(new int[10], new TreeSet<>(List.of(1)));
        for (int gid : result) {
            assertThat(gid).isEqualTo(1);
        }
    }

    @Test
    void secondGroupTakesHalfAndOnlyHalfMoves() {
        int[] one = Rebalancer.rebalance(new int[10], new TreeSet<>(List.of(1)));
        int[] two = Rebalancer.rebalance(one, new TreeSet<>(List.of(1, 2)));
        assertThat(Rebalancer.moves(one, two)).isEqualTo(5);
        assertThat(count(two, 1)).isEqualTo(5);
        assertThat(count(two, 2)).isEqualTo(5);
    }

    @Test
    void aDepartingGroupsShardsGoToTheOthers() {
        int[] three = Rebalancer.rebalance(new int[9], new TreeSet<>(List.of(1, 2, 3)));
        int[] two = Rebalancer.rebalance(three, new TreeSet<>(List.of(1, 3)));
        assertThat(count(two, 2)).isEqualTo(0);
        assertThat(Rebalancer.moves(three, two)).isEqualTo(3); // exactly group 2's shards move
    }

    @Test
    void noGroupsMeansEverythingUnassigned() {
        int[] result = Rebalancer.rebalance(new int[] {1, 2, 1}, new TreeSet<>());
        assertThat(count(result, 0)).isEqualTo(3);
    }

    @Test
    void moreGroupsThanShardsGivesSomeGroupsNothing() {
        int[] result = Rebalancer.rebalance(new int[] {0, 0, 0}, new TreeSet<>(List.of(1, 2, 3, 4, 5)));
        for (int gid : result) {
            assertThat(gid).isGreaterThan(0);
        }
        assertThat(new TreeSet<>(List.of(result[0], result[1], result[2])).size()).as("one shard each").isEqualTo(3);
    }

    @Property(tries = 2000)
    void balancedMinimalDeterministicAndOnlyOnLiveGroups(@ForAll long seed) {
        Random r = new Random(seed);
        int n = 1 + r.nextInt(64);
        int[] current = new int[n];
        for (int s = 0; s < n; s++) {
            current[s] = r.nextInt(4) == 0 ? 0 : 1 + r.nextInt(8);
        }
        SortedSet<Integer> gids = new TreeSet<>();
        int g = r.nextInt(9);
        while (gids.size() < g) {
            gids.add(1 + r.nextInt(8));
        }
        int[] result = Rebalancer.rebalance(current, gids);

        // Deterministic, and already balanced input stays put.
        assertThat(Rebalancer.rebalance(current, gids)).isEqualTo(result);
        assertThat(Rebalancer.moves(result, Rebalancer.rebalance(result, gids))).as("seed %d: idempotent", seed).isEqualTo(0);

        if (gids.isEmpty()) {
            assertThat(count(result, 0)).isEqualTo(n);
            return;
        }
        // Only live groups own shards.
        for (int gid : result) {
            assertThat(gids.contains(gid)).as("seed %d: shard on group %d", seed, gid).isTrue();
        }
        // Balanced: floor(N/G) or ceil(N/G), with exactly N mod G at the ceiling.
        int base = n / gids.size();
        int atCeiling = 0;
        for (int gid : gids) {
            int c = count(result, gid);
            assertThat(c == base || c == base + 1).as("seed %d: group %d has %d", seed, gid, c).isTrue();
            if (c == base + 1) {
                atCeiling++;
            }
        }
        assertThat(atCeiling).isEqualTo(n % gids.size());
        // Minimal moves: no assignment satisfying the above keeps more shards in place.
        assertThat(Rebalancer.moves(current, result)).as("seed %d: moves", seed).isEqualTo(minimumMoves(current, gids));
    }

    /** N minus the most shards any balanced assignment can keep: the extra slots go to the biggest holders. */
    private static int minimumMoves(int[] current, SortedSet<Integer> gids) {
        Map<Integer, Integer> held = new TreeMap<>();
        for (int gid : gids) {
            held.put(gid, count(current, gid));
        }
        List<Integer> counts = new ArrayList<>(held.values());
        counts.sort((a, b) -> b - a);
        int n = current.length;
        int base = n / gids.size();
        int extra = n % gids.size();
        int kept = 0;
        for (int i = 0; i < counts.size(); i++) {
            kept += Math.min(counts.get(i), base + (i < extra ? 1 : 0));
        }
        return n - kept;
    }

    private static int count(int[] assignment, int gid) {
        int c = 0;
        for (int g : assignment) {
            if (g == gid) {
                c++;
            }
        }
        return c;
    }
}
