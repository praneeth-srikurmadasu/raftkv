package dev.raftkv.transport.sim;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import dev.raftkv.codec.Frame;
import dev.raftkv.codec.FrameCodec;
import dev.raftkv.codec.FrameDecoder;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.Message;
import dev.raftkv.protocol.MessageCodec;
import dev.raftkv.sim.Simulation;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.MessageHandler;
import dev.raftkv.transport.Transport;

/**
 * An in-memory network for simulations (SPEC §5.3): deterministic, and able to misbehave on
 * purpose.
 *
 * <p>Each sent message becomes a delivery event in the {@link Simulation}, after a random
 * latency drawn from the network's own seeded random source. Because latencies differ,
 * messages get reordered naturally. On top of that the network can:
 * <ul>
 *   <li>drop a fraction of messages ({@link #setDropRate}),</li>
 *   <li>duplicate a fraction of messages ({@link #setDuplicateRate}),</li>
 *   <li>split nodes into partitions that cannot reach each other ({@link #partition}, {@link #isolate}, {@link #heal}),</li>
 *   <li>take a node down so it neither sends nor receives ({@link #setNodeUp}),</li>
 *   <li>break a link in one direction only ({@link #blockOneWay}): A's messages to B are lost
 *       while B's messages to A still arrive. Asymmetric failures are a classic source of bugs,
 *       e.g. a leader that receives votes but whose heartbeats never arrive.</li>
 * </ul>
 * Link state is checked both when a message is sent and when it would be delivered, so a
 * partition that starts while a message is in flight drops it (like a broken TCP connection).
 *
 * <p>By default every message is encoded into a real frame and decoded again on delivery.
 * That runs the production codec on every simulated message and guarantees that nodes
 * never share objects in memory, just as with a real network.
 *
 * <p>Disk faults live in {@code SimulatedDisk}; clock skew (for lease reads) arrives with Milestone 11.
 */
public final class SimulatedNetwork {

    private final Simulation sim;
    private final Random random;
    private final TreeMap<NodeId, MessageHandler> handlers = new TreeMap<>();
    private final Set<NodeId> down = new HashSet<>();
    /** Partition id per node; nodes with different ids cannot talk. Absent = default partition 0. */
    private final TreeMap<NodeId, Integer> partitionOf = new TreeMap<>();
    /** Directed links that are down: "from->to". */
    private final java.util.TreeSet<String> blockedLinks = new java.util.TreeSet<>();

    private long minLatencyNanos = Duration.ofMillis(1).toNanos();
    private long maxLatencyNanos = Duration.ofMillis(10).toNanos();
    private double dropRate;
    private double duplicateRate;
    private boolean encodeMessages = true;

    private long sent;
    private long delivered;
    private long dropped;
    private long duplicated;

    public SimulatedNetwork(Simulation sim) {
        this.sim = Objects.requireNonNull(sim, "sim");
        this.random = sim.forkRandom();
    }

    // ------------------------------------------------------------------ wiring

    /** Registers {@code node} with the handler that receives its messages, and returns its transport. */
    public Transport register(NodeId node, MessageHandler handler) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(handler, "handler");
        if (handlers.putIfAbsent(node, handler) != null) {
            throw new IllegalArgumentException(node + " is already registered");
        }
        return new Transport() {
            @Override
            public NodeId self() {
                return node;
            }

            @Override
            public void send(NodeId to, GroupId group, Message message) {
                SimulatedNetwork.this.send(new Envelope(node, to, group, message));
            }
        };
    }

    /** Replaces the handler of an already registered node (e.g. a restarted node with fresh state). */
    public void replaceHandler(NodeId node, MessageHandler handler) {
        if (!handlers.containsKey(node)) {
            throw new IllegalArgumentException(node + " is not registered");
        }
        handlers.put(node, Objects.requireNonNull(handler, "handler"));
    }

    /** Registered nodes in id order. */
    public List<NodeId> nodes() {
        return List.copyOf(handlers.keySet());
    }

    // ------------------------------------------------------------------ configuration

    /** Each message takes a uniformly random latency in {@code [min, max]}. */
    public void setLatency(Duration min, Duration max) {
        if (min.isNegative() || max.compareTo(min) < 0) {
            throw new IllegalArgumentException("need 0 <= min <= max, got " + min + ", " + max);
        }
        this.minLatencyNanos = min.toNanos();
        this.maxLatencyNanos = max.toNanos();
    }

    /** Probability in [0, 1] that a message is silently lost. */
    public void setDropRate(double probability) {
        this.dropRate = checkProbability(probability);
    }

    /** Probability in [0, 1] that a delivered message is delivered a second time (later). */
    public void setDuplicateRate(double probability) {
        this.duplicateRate = checkProbability(probability);
    }

    /** Whether messages go through the real frame codec (default true). */
    public void setEncodeMessages(boolean encode) {
        this.encodeMessages = encode;
    }

    // ------------------------------------------------------------------ faults

    /** A down node neither sends nor receives; in-flight messages to it are dropped. */
    public void setNodeUp(NodeId node, boolean up) {
        requireRegistered(node);
        if (up) {
            down.remove(node);
        } else {
            down.add(node);
        }
        sim.trace("net " + node + (up ? " up" : " down"));
    }

    public boolean isUp(NodeId node) {
        return !down.contains(node);
    }

    /**
     * Splits the network: nodes in different groups cannot reach each other. Nodes not listed
     * end up together in one extra partition. Replaces any previous partitioning.
     */
    @SafeVarargs
    public final void partition(Collection<NodeId>... groups) {
        partitionOf.clear();
        int id = 1;
        for (Collection<NodeId> group : groups) {
            for (NodeId node : group) {
                requireRegistered(node);
                if (partitionOf.put(node, id) != null) {
                    throw new IllegalArgumentException(node + " appears in more than one partition");
                }
            }
            id++;
        }
        sim.trace("net partition " + partitionOf);
    }

    /** Cuts {@code node} off from everyone else. */
    public void isolate(NodeId node) {
        requireRegistered(node);
        partition(List.of(node));
    }

    /** Drops every message from {@code from} to {@code to}; the opposite direction still works. */
    public void blockOneWay(NodeId from, NodeId to) {
        requireRegistered(from);
        requireRegistered(to);
        blockedLinks.add(from + "->" + to);
        sim.trace("net block " + from + "->" + to);
    }

    /** Removes all partitions and one-way blocks (down nodes stay down). */
    public void heal() {
        partitionOf.clear();
        blockedLinks.clear();
        sim.trace("net heal");
    }

    /** True if a message from {@code from} to {@code to} can currently get through. */
    public boolean canReach(NodeId from, NodeId to) {
        return isUp(from) && isUp(to)
                && partitionOf.getOrDefault(from, 0).equals(partitionOf.getOrDefault(to, 0))
                && !blockedLinks.contains(from + "->" + to);
    }

    // ------------------------------------------------------------------ stats

    public long sentCount() {
        return sent;
    }

    public long deliveredCount() {
        return delivered;
    }

    public long droppedCount() {
        return dropped;
    }

    public long duplicatedCount() {
        return duplicated;
    }

    // ------------------------------------------------------------------ internals

    private void send(Envelope envelope) {
        NodeId from = envelope.from();
        NodeId to = envelope.to();
        if (from.equals(to)) {
            throw new IllegalArgumentException(from + " tried to send a message to itself");
        }
        requireRegistered(to);
        sent++;
        String what = envelope.message().type() + " " + from + "->" + to + " " + envelope.group();
        if (!canReach(from, to) || random.nextDouble() < dropRate) {
            dropped++;
            sim.trace("net drop " + what);
            return;
        }
        sim.trace("net send " + what);
        scheduleDelivery(envelope, what);
        if (random.nextDouble() < duplicateRate) {
            duplicated++;
            sim.trace("net dup " + what);
            scheduleDelivery(envelope, what);
        }
    }

    private void scheduleDelivery(Envelope envelope, String what) {
        // Encode now, decode at delivery: the receiver gets its own copy built from bytes.
        byte[] wire = encodeMessages
                ? FrameCodec.encodeToArray(MessageCodec.toFrame(envelope.group(), 0, envelope.message()))
                : null;
        sim.schedule(Duration.ofNanos(latencyNanos()), () -> deliver(envelope, wire, what));
    }

    private void deliver(Envelope original, byte[] wire, String what) {
        if (!canReach(original.from(), original.to())) {
            dropped++;
            sim.trace("net drop-in-flight " + what);
            return;
        }
        Envelope envelope = original;
        if (wire != null) {
            List<Frame> frames = new FrameDecoder().feed(wire);
            if (frames.size() != 1) {
                throw new IllegalStateException("expected exactly one frame, got " + frames.size());
            }
            Frame frame = frames.get(0);
            envelope = new Envelope(original.from(), original.to(), GroupId.of(frame.groupId()),
                    MessageCodec.fromFrame(frame));
        }
        delivered++;
        sim.trace("net deliver " + what);
        handlers.get(original.to()).onMessage(envelope);
    }

    private long latencyNanos() {
        if (maxLatencyNanos == minLatencyNanos) {
            return minLatencyNanos;
        }
        return minLatencyNanos + (long) (random.nextDouble() * (maxLatencyNanos - minLatencyNanos + 1));
    }

    private void requireRegistered(NodeId node) {
        if (!handlers.containsKey(node)) {
            throw new IllegalArgumentException(node + " is not registered with the network");
        }
    }

    private static double checkProbability(double p) {
        if (!(p >= 0.0 && p <= 1.0)) {
            throw new IllegalArgumentException("probability must be in [0, 1], was " + p);
        }
        return p;
    }
}
