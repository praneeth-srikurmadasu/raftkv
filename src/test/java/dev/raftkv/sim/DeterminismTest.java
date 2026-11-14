package dev.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.RequestVote;
import dev.raftkv.protocol.RequestVoteReply;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;
import dev.raftkv.transport.sim.SimulatedNetwork;
import org.junit.jupiter.api.Test;

/**
 * The Milestone 1 gate (SPEC §3.2): the same seed must produce the same execution, event for event.
 *
 * <p>The scenario is a noisy 5-node cluster: every node sends a RequestVote to a random peer
 * every 20-80 ms and answers every RequestVote with a reply, while the network drops,
 * duplicates and reorders messages, gets partitioned, heals, and loses a node for a while.
 * Every send/drop/delivery is traced; the trace hash fingerprints the whole run.
 */
class DeterminismTest {

    record Outcome(long traceHash, long delivered, long dropped, long eventsRun) {}

    static Outcome runScenario(long seed) {
        Simulation sim = new Simulation(seed);
        SimulatedNetwork net = new SimulatedNetwork(sim);
        net.setLatency(Duration.ofMillis(1), Duration.ofMillis(30));
        net.setDropRate(0.1);
        net.setDuplicateRate(0.05);

        List<NodeId> nodes = List.of(NodeId.of(1), NodeId.of(2), NodeId.of(3), NodeId.of(4), NodeId.of(5));
        Map<NodeId, Transport> transports = new TreeMap<>();
        GroupId group = GroupId.of(1);

        for (NodeId self : nodes) {
            transports.put(self, net.register(self, (Envelope e) -> {
                if (e.message() instanceof RequestVote rv) {
                    transports.get(self).send(e.from(), group, new RequestVoteReply(rv.term(), true, false));
                }
            }));
        }
        // Each node runs its own timer, like a heartbeat: every 20-80 ms it messages a random peer.
        for (NodeId self : nodes) {
            startTicking(sim, self, nodes, transports.get(self), sim.forkRandom(), group);
        }

        sim.schedule(Duration.ofSeconds(2), () -> net.partition(nodes.subList(0, 2), nodes.subList(2, 5)));
        sim.schedule(Duration.ofSeconds(4), net::heal);
        sim.schedule(Duration.ofSeconds(5), () -> net.setNodeUp(nodes.get(2), false));
        sim.schedule(Duration.ofSeconds(6), () -> net.setNodeUp(nodes.get(2), true));
        sim.runFor(Duration.ofSeconds(10));

        return new Outcome(sim.traceHash(), net.deliveredCount(), net.droppedCount(), sim.eventsRun());
    }

    private static void startTicking(Simulation sim, NodeId self, List<NodeId> nodes,
                                     Transport transport, Random random, GroupId group) {
        sim.schedule(Duration.ofMillis(20 + random.nextInt(61)), () -> {
            NodeId peer;
            do {
                peer = nodes.get(random.nextInt(nodes.size()));
            } while (peer.equals(self));
            transport.send(peer, group, new RequestVote(1 + random.nextInt(10), self, 0, 0, false, false));
            startTicking(sim, self, nodes, transport, random, group); // schedule the next tick
        });
    }

    @Test
    void sameSeedGivesTheIdenticalRun() {
        long seed = Seeds.resolve();
        Outcome first = runScenario(seed);
        Outcome second = runScenario(seed);
        assertThat(second).as("seed %d (re-run with -Draftkv.seed=%d)", seed, seed).isEqualTo(first);
        assertThat(first.delivered()).as("seed %d", seed).isGreaterThan(500L); // the scenario really did something
        assertThat(first.dropped()).as("seed %d", seed).isGreaterThan(0L);
    }

    @Test
    void manySeedsAreEachReproducibleAndDifferentFromEachOther() {
        Set<Long> hashes = new HashSet<>();
        for (long seed = 1; seed <= 30; seed++) {
            Outcome a = runScenario(seed);
            Outcome b = runScenario(seed);
            assertThat(b).as("seed %d", seed).isEqualTo(a);
            hashes.add(a.traceHash());
        }
        assertThat(hashes).hasSize(30); // different seeds explore different executions
    }
}
