package dev.raftkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import dev.raftkv.common.NodeId;
import dev.raftkv.sim.SimulationException;

/**
 * One randomized chaos run for leader election (used by the chaos test and the bug-detection test).
 *
 * <p>Swarm-style (SPEC §9.5): the seed picks the cluster size and which kinds of faults are
 * active and how strong they are, so different seeds stress different combinations.
 *
 * <ol>
 *   <li><b>Chaos phase (10 s):</b> latency, drops, duplicates, and a "nemesis" that every
 *       100-600 ms partitions the network, isolates a node, heals, crashes a node or restarts
 *       one. Safety invariants are checked after every single event.</li>
 *   <li><b>Recovery phase:</b> heal everything, restart every crashed node, stop dropping
 *       messages, and let the group settle for 2 s. It must then converge on exactly one
 *       leader within 10 s (liveness), and that leader must stay in charge for another 5 s
 *       (stability).</li>
 * </ol>
 */
final class ElectionChaos {

    private ElectionChaos() {
    }

    /** Outcome of one run: {@code failure} is empty when every check passed. */
    record Result(long seed, int size, Optional<String> failure, int termsWithLeader, long checks) {
        boolean passed() {
            return failure.isEmpty();
        }
    }

    static Result run(long seed, RaftConfig config) {
        Random r = new Random(seed);
        int size = r.nextBoolean() ? 3 : 5;
        SimulatedCluster c = new SimulatedCluster(seed, size, config);
        try {
            boolean partitions = r.nextInt(4) != 0;
            boolean crashes = r.nextInt(4) != 0;
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(5 + r.nextInt(26)));
            c.net.setDropRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.15);
            c.net.setDuplicateRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.1);
            c.startAll();

            Random nemesis = c.sim.forkRandom();
            long chaosEnd = Duration.ofSeconds(10).toNanos();
            while (c.sim.now() < chaosEnd) {
                c.run(Duration.ofMillis(100 + nemesis.nextInt(501)));
                act(c, nemesis, partitions, crashes);
            }

            // Recovery: everything back to normal.
            c.net.heal();
            for (NodeId id : c.crashedNodes()) {
                c.restart(id);
            }
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            // A node that missed heartbeats while cut off may time out right after the heal and
            // force one more election (its higher term makes the leader step down). That is
            // expected without pre-vote (Milestone 11), so let the group settle first.
            c.run(Duration.ofSeconds(2));
            RaftNode leader = c.awaitLeader(Duration.ofSeconds(10));
            long term = leader.currentTerm();
            c.run(Duration.ofSeconds(5));
            Optional<RaftNode> after = c.convergedLeader(c.ids);
            if (after.isEmpty() || after.get() != leader || leader.currentTerm() != term) {
                throw new AssertionError("leader not stable on a healthy network: was " + leader.id() + "@t" + term
                        + ", now " + c.runningNodes());
            }
            return new Result(seed, size, Optional.empty(), c.invariants.termsWithLeader(), c.invariants.checks());
        } catch (AssertionError | SimulationException | IllegalStateException e) {
            String message = e.getMessage() + (e.getCause() != null ? " / cause: " + e.getCause().getMessage() : "");
            return new Result(seed, size, Optional.of(message), c.invariants.termsWithLeader(), c.invariants.checks());
        }
    }

    /** One random nemesis action (shared with {@link ReplicationChaos}). */
    static void act(SimulatedCluster c, Random r, boolean partitions, boolean crashes) {
        int choice = r.nextInt(6);
        List<NodeId> ids = c.ids;
        if (partitions && choice == 0) {
            List<NodeId> shuffled = new ArrayList<>(ids);
            java.util.Collections.shuffle(shuffled, r);
            int cut = 1 + r.nextInt(ids.size() - 1);
            c.net.partition(shuffled.subList(0, cut), shuffled.subList(cut, shuffled.size()));
        } else if (partitions && choice == 1) {
            c.net.isolate(ids.get(r.nextInt(ids.size())));
        } else if (choice == 2) {
            c.net.heal();
        } else if (crashes && choice == 3) {
            // Prefer crashing the leader: that is where elections get interesting.
            List<RaftNode> leaders = c.leaders();
            NodeId victim = !leaders.isEmpty() && r.nextBoolean()
                    ? leaders.get(r.nextInt(leaders.size())).id()
                    : ids.get(r.nextInt(ids.size()));
            if (c.isRunning(victim)) {
                c.crash(victim);
            }
        } else if (crashes && choice >= 4) {
            List<NodeId> crashed = c.crashedNodes();
            if (!crashed.isEmpty()) {
                c.restart(crashed.get(r.nextInt(crashed.size())));
            }
        }
    }
}
