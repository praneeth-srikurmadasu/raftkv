package dev.raftkv.shard;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * Spreads shards evenly over replica groups while moving as few as possible (SPEC §6.3). A pure,
 * deterministic function: every controller replica computes the same result.
 *
 * <p>With N shards and G groups, every group ends with ⌊N/G⌋ or ⌈N/G⌉ shards. Exactly
 * {@code N mod G} groups get the extra one; they are the groups that hold the most shards now (ties:
 * lowest gid), because that keeps the most shards where they are. Then:
 * <ol>
 *   <li>shards on gid 0 or on groups that are gone are free;</li>
 *   <li>every group above its target gives up its highest-numbered shards;</li>
 *   <li>groups below their target, lowest gid first, take free shards, lowest-numbered first.</li>
 * </ol>
 * Every move is forced (a shard on a departed group, or a surplus shard), so the number of moves
 * is the minimum possible.
 */
public final class Rebalancer {

    private Rebalancer() {
    }

    /**
     * @param current the current shard → gid assignment
     * @param gids    the groups of the new configuration (empty: everything becomes unassigned)
     * @return the new assignment
     */
    public static int[] rebalance(int[] current, SortedSet<Integer> gids) {
        int n = current.length;
        int[] result = current.clone();
        if (gids.isEmpty()) {
            return new int[n];
        }
        Map<Integer, List<Integer>> holdings = new TreeMap<>();
        for (int gid : gids) {
            holdings.put(gid, new ArrayList<>());
        }
        Deque<Integer> free = new ArrayDeque<>();
        for (int s = 0; s < n; s++) {
            List<Integer> h = holdings.get(current[s]);
            if (h == null) {
                free.add(s); // unassigned, or on a group that left
            } else {
                h.add(s);
            }
        }
        Map<Integer, Integer> target = targets(holdings, n);
        List<Integer> released = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> e : holdings.entrySet()) {
            List<Integer> h = e.getValue();
            int t = target.get(e.getKey());
            while (h.size() > t) {
                released.add(h.remove(h.size() - 1)); // highest-numbered first
            }
        }
        released.sort(null);
        List<Integer> pool = new ArrayList<>(free);
        pool.addAll(released);
        pool.sort(null);
        Deque<Integer> queue = new ArrayDeque<>(pool);
        for (Map.Entry<Integer, List<Integer>> e : holdings.entrySet()) {
            int gid = e.getKey();
            int t = target.get(gid);
            while (e.getValue().size() < t) {
                int s = queue.removeFirst();
                e.getValue().add(s);
                result[s] = gid;
            }
        }
        if (!queue.isEmpty()) {
            throw new IllegalStateException("rebalance left shards unassigned: " + queue);
        }
        return result;
    }

    /** ⌊N/G⌋ for every group, plus one for the {@code N mod G} groups that hold the most now (ties: lowest gid). */
    static Map<Integer, Integer> targets(Map<Integer, List<Integer>> holdings, int n) {
        int g = holdings.size();
        int base = n / g;
        int extra = n % g;
        List<Integer> order = new ArrayList<>(holdings.keySet());
        order.sort(Comparator.<Integer>comparingInt(gid -> -holdings.get(gid).size()).thenComparingInt(gid -> gid));
        Map<Integer, Integer> target = new TreeMap<>();
        for (int i = 0; i < order.size(); i++) {
            target.put(order.get(i), base + (i < extra ? 1 : 0));
        }
        return target;
    }

    /** How many shards change owner between two assignments (tests, logging). */
    public static int moves(int[] before, int[] after) {
        int m = 0;
        for (int s = 0; s < before.length; s++) {
            if (before[s] != after[s]) {
                m++;
            }
        }
        return m;
    }
}
