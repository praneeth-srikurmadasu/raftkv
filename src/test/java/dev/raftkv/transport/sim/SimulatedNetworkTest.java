package dev.raftkv.transport.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.InstallSnapshotReply;
import dev.raftkv.protocol.Message;
import dev.raftkv.protocol.RequestVote;
import dev.raftkv.sim.Simulation;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;
import org.junit.jupiter.api.Test;

class SimulatedNetworkTest {

    private static final NodeId A = NodeId.of(1);
    private static final NodeId B = NodeId.of(2);
    private static final NodeId C = NodeId.of(3);
    private static final GroupId G = GroupId.of(1);

    /** A small test cluster: every node records what it receives, with the arrival time. */
    private static final class Fixture {
        final Simulation sim;
        final SimulatedNetwork net;
        final Map<NodeId, Transport> transports = new TreeMap<>();
        final Map<NodeId, List<Envelope>> inbox = new TreeMap<>();
        final List<Long> arrivalMillis = new ArrayList<>();

        Fixture(long seed) {
            sim = new Simulation(seed);
            net = new SimulatedNetwork(sim);
            for (NodeId n : List.of(A, B, C)) {
                inbox.put(n, new ArrayList<>());
                transports.put(n, net.register(n, e -> {
                    inbox.get(n).add(e);
                    arrivalMillis.add(sim.clock().millis());
                }));
            }
        }

        void send(NodeId from, NodeId to, Message m) {
            transports.get(from).send(to, G, m);
        }
    }

    private static Message msg(long term) {
        return new InstallSnapshotReply(term);
    }

    @Test
    void deliversAfterTheConfiguredLatency() {
        Fixture f = new Fixture(1);
        f.net.setLatency(Duration.ofMillis(5), Duration.ofMillis(5));
        RequestVote rv = new RequestVote(3, A, 10, 2, false, false);
        f.send(A, B, rv);
        f.sim.runFor(Duration.ofMillis(4));
        assertThat(f.inbox.get(B)).isEmpty();
        f.sim.runFor(Duration.ofMillis(1));

        assertThat(f.inbox.get(B)).hasSize(1);
        Envelope e = f.inbox.get(B).get(0);
        assertThat(e.from()).isEqualTo(A);
        assertThat(e.to()).isEqualTo(B);
        assertThat(e.group()).isEqualTo(G);
        assertThat(e.message()).isEqualTo(rv);
        assertThat(e.message()).isNotSameAs(rv); // rebuilt from bytes, like a real network
        assertThat(f.arrivalMillis).isEqualTo(List.of(5L));
    }

    @Test
    void latencyStaysWithinBoundsAndReordersMessages() {
        Fixture f = new Fixture(2);
        f.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(50));
        for (int i = 1; i <= 100; i++) {
            f.send(A, B, msg(i));
        }
        f.sim.runFor(Duration.ofSeconds(1));
        List<Long> received = new ArrayList<>();
        f.inbox.get(B).forEach(e -> received.add(e.message().term()));
        assertThat(received).hasSize(100);
        assertThat(received).isNotEqualTo(received.stream().sorted().toList()); // some arrived out of order
        f.arrivalMillis.forEach(t -> assertThat(t).isBetween(1L, 50L));
    }

    @Test
    void dropRateOneLosesEverything() {
        Fixture f = new Fixture(3);
        f.net.setDropRate(1.0);
        for (int i = 0; i < 10; i++) {
            f.send(A, B, msg(i));
        }
        f.sim.runFor(Duration.ofSeconds(1));
        assertThat(f.inbox.get(B)).isEmpty();
        assertThat(f.net.droppedCount()).isEqualTo(10L);
        assertThat(f.net.sentCount()).isEqualTo(10L);
    }

    @Test
    void duplicateRateOneDeliversTwice() {
        Fixture f = new Fixture(4);
        f.net.setDuplicateRate(1.0);
        f.send(A, B, msg(7));
        f.sim.runFor(Duration.ofSeconds(1));
        assertThat(f.inbox.get(B)).hasSize(2);
        assertThat(f.net.duplicatedCount()).isEqualTo(1L);
    }

    @Test
    void partitionsBlockTrafficBetweenSidesUntilHealed() {
        Fixture f = new Fixture(5);
        f.net.partition(List.of(A), List.of(B, C));
        f.send(A, B, msg(1));      // across: blocked
        f.send(B, A, msg(2));      // across, other direction: blocked
        f.send(B, C, msg(3));      // same side: fine
        f.sim.runFor(Duration.ofSeconds(1));
        assertThat(f.inbox.get(A)).isEmpty();
        assertThat(f.inbox.get(B)).isEmpty();
        assertThat(f.inbox.get(C)).hasSize(1);
        assertThat(f.net.canReach(A, B)).isFalse();

        f.net.heal();
        f.send(A, B, msg(4));
        f.sim.runFor(Duration.ofSeconds(1));
        assertThat(f.inbox.get(B)).hasSize(1);
    }

    @Test
    void isolateCutsOneNodeOffFromEveryone() {
        Fixture f = new Fixture(6);
        f.net.isolate(C);
        assertThat(f.net.canReach(A, B)).isTrue();
        assertThat(f.net.canReach(A, C)).isFalse();
        assertThat(f.net.canReach(C, B)).isFalse();
    }

    @Test
    void partitionStartingWhileAMessageIsInFlightDropsIt() {
        Fixture f = new Fixture(7);
        f.net.setLatency(Duration.ofMillis(10), Duration.ofMillis(10));
        f.send(A, B, msg(1));
        f.sim.runFor(Duration.ofMillis(5));
        f.net.isolate(B);
        f.sim.runFor(Duration.ofMillis(10));
        assertThat(f.inbox.get(B)).isEmpty();
        assertThat(f.net.droppedCount()).isEqualTo(1L);
    }

    @Test
    void downNodesNeitherSendNorReceive() {
        Fixture f = new Fixture(8);
        f.net.setNodeUp(B, false);
        f.send(A, B, msg(1));
        f.send(B, C, msg(2));
        f.sim.runFor(Duration.ofSeconds(1));
        assertThat(f.inbox.get(B)).isEmpty();
        assertThat(f.inbox.get(C)).isEmpty();

        f.net.setNodeUp(B, true);
        f.send(A, B, msg(3));
        f.sim.runFor(Duration.ofSeconds(1));
        assertThat(f.inbox.get(B)).hasSize(1);
    }

    @Test
    void rejectsProgrammingErrors() {
        Fixture f = new Fixture(9);
        assertThatThrownBy(() -> f.send(A, A, msg(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> f.transports.get(A).send(NodeId.of(42), G, msg(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> f.net.register(A, e -> { })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> f.net.setDropRate(1.5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> f.net.setDropRate(Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> f.net.partition(List.of(A), List.of(A, B))).isInstanceOf(IllegalArgumentException.class);
    }
}
