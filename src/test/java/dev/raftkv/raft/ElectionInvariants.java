package dev.raftkv.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import dev.raftkv.common.NodeId;

/**
 * Safety properties of leader election, checked after every simulated event (SPEC §9.3).
 *
 * <p>The checker keeps its own history, independent of the nodes, so a violation is caught
 * even if the offending node has since crashed:
 * <ul>
 *   <li><b>Election Safety</b> (Figure 3): at most one leader can be elected in a given term.</li>
 *   <li><b>One vote per term</b> (§5.2): "each server will vote for at most one candidate in a
 *       given term". This is the rule Election Safety rests on, so checking it directly
 *       catches bugs before they happen to produce two leaders.</li>
 *   <li><b>Monotonic terms</b>: a server's currentTerm never decreases, even across restarts.</li>
 * </ul>
 */
final class ElectionInvariants implements Runnable {

    private final SimulatedCluster cluster;
    private final Map<Long, NodeId> leaderOfTerm = new HashMap<>();
    private final Map<NodeId, Map<Long, NodeId>> voteOfTerm = new TreeMap<>();
    private final Map<NodeId, Long> highestTerm = new TreeMap<>();
    private long checks;

    ElectionInvariants(SimulatedCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        checks++;
        for (RaftNode n : cluster.runningNodes()) {
            long term = n.currentTerm();

            Long previous = highestTerm.put(n.id(), Math.max(term, highestTerm.getOrDefault(n.id(), 0L)));
            if (previous != null && term < previous) {
                throw new AssertionError("Monotonic terms violated: " + n.id() + " went from t" + previous + " to t" + term);
            }

            if (n.role() == Role.LEADER) {
                NodeId existing = leaderOfTerm.putIfAbsent(term, n.id());
                if (existing != null && !existing.equals(n.id())) {
                    throw new AssertionError("Election Safety violated: both " + existing + " and " + n.id()
                            + " were leader in t" + term);
                }
            }

            n.votedFor().ifPresent(vote -> {
                NodeId earlier = voteOfTerm.computeIfAbsent(n.id(), k -> new HashMap<>()).putIfAbsent(term, vote);
                if (earlier != null && !earlier.equals(vote)) {
                    throw new AssertionError("One-vote-per-term violated: " + n.id() + " voted for " + earlier
                            + " and then " + vote + " in t" + term);
                }
            });
        }
    }

    long checks() {
        return checks;
    }

    int termsWithLeader() {
        return leaderOfTerm.size();
    }
}
