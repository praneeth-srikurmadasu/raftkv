package dev.raftkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.raft.storage.InMemoryMetaStore;
import dev.raftkv.raft.storage.PersistentState;
import dev.raftkv.sim.Simulation;
import dev.raftkv.transport.Transport;
import dev.raftkv.transport.sim.SimulatedNetwork;

/**
 * A whole Raft group running inside one {@link Simulation}.
 *
 * <p>Each node has <em>durable</em> state (its {@link InMemoryMetaStore} and log,
 * which the cluster keeps) and <em>volatile</em> state (the {@link RaftNode} object). Crashing
 * a node throws the RaftNode away and takes it off the network; restarting builds a fresh
 * RaftNode on the same durable state, exactly like a process restart on the same disk.
 *
 * <p>{@link ElectionInvariants} and {@link ReplicationInvariants} run after every simulated
 * event; State Machine Safety is checked by each node's {@link RecordingStateMachine}. The
 * state machine is volatile, like {@code commitIndex}: a restarted node re-applies its log.
 */
final class SimulatedCluster {

    static final GroupId GROUP = GroupId.of(1);

    final Simulation sim;
    final SimulatedNetwork net;
    final RaftConfig config;
    final List<NodeId> ids;
    final ElectionInvariants invariants;
    final ReplicationInvariants replicationInvariants;
    /** index -> command, shared by every replica's state machine to check State Machine Safety. */
    final Map<Long, Bytes> globallyApplied = new HashMap<>();

    private final Map<NodeId, InMemoryMetaStore> metaStores = new TreeMap<>();
    private final Map<NodeId, TrackingLog> logs = new TreeMap<>();
    private final Map<NodeId, RecordingStateMachine> stateMachines = new TreeMap<>();
    private final Map<NodeId, Transport> transports = new TreeMap<>();
    private final Map<NodeId, RaftNode> running = new TreeMap<>();
    /** Optional hook that sees every message as it is delivered (tests count specific messages). */
    java.util.function.Consumer<dev.raftkv.transport.Envelope> deliveryObserver = e -> { };

    SimulatedCluster(long seed, int size, RaftConfig config) {
        this(seed, size, config, Map.of(), 0);
    }

    /**
     * @param initialLogs log contents per node before the first start (for stale-log scenarios)
     * @param initialTerm currentTerm every node starts with (must be >= every initial log term)
     */
    SimulatedCluster(long seed, int size, RaftConfig config, Map<NodeId, List<LogEntry>> initialLogs, long initialTerm) {
        this.sim = new Simulation(seed);
        this.net = new SimulatedNetwork(sim);
        this.config = config;
        List<NodeId> list = new ArrayList<>();
        for (int i = 1; i <= size; i++) {
            list.add(NodeId.of(i));
        }
        this.ids = List.copyOf(list);
        for (NodeId id : ids) {
            metaStores.put(id, new InMemoryMetaStore(new PersistentState(initialTerm, null)));
            logs.put(id, new TrackingLog(initialLogs.getOrDefault(id, List.of())));
            // The handler looks up whichever RaftNode currently runs as this id.
            transports.put(id, net.register(id, envelope -> {
                RaftNode node = running.get(id);
                if (node != null) {
                    deliveryObserver.accept(envelope);
                    node.handle(envelope);
                }
            }));
        }
        this.invariants = new ElectionInvariants(this);
        this.replicationInvariants = new ReplicationInvariants(this);
        sim.addInvariant(invariants);
        sim.addInvariant(replicationInvariants);
    }

    void startAll() {
        ids.forEach(this::start);
    }

    /** Starts every node except {@code down}, which stays crashed (off the network) until restarted. */
    void startAllExcept(Collection<NodeId> down) {
        for (NodeId id : ids) {
            if (down.contains(id)) {
                net.setNodeUp(id, false);
            } else {
                start(id);
            }
        }
    }

    private void start(NodeId id) {
        RecordingStateMachine sm = new RecordingStateMachine(id, globallyApplied);
        stateMachines.put(id, sm);
        RaftNode node = RaftNode.builder()
                .self(id)
                .group(GROUP)
                .voters(new TreeSet<>(ids))
                .config(config)
                .scheduler(sim)
                .clock(sim.clock())
                .random(sim.forkRandom())
                .transport(transports.get(id))
                .log(logs.get(id))
                .metaStore(metaStores.get(id))
                .stateMachine(sm)
                .tracer(sim)
                .build();
        running.put(id, node);
        node.start();
    }

    /** Kills the node: volatile state is gone, durable state stays, and it drops off the network. */
    void crash(NodeId id) {
        RaftNode node = running.remove(id);
        if (node == null) {
            throw new IllegalStateException(id + " is not running");
        }
        node.stop();
        net.setNodeUp(id, false);
    }

    /** Starts a fresh RaftNode on the crashed node's durable state. */
    void restart(NodeId id) {
        if (running.containsKey(id)) {
            throw new IllegalStateException(id + " is already running");
        }
        net.setNodeUp(id, true);
        start(id);
    }

    boolean isRunning(NodeId id) {
        return running.containsKey(id);
    }

    RaftNode node(NodeId id) {
        RaftNode node = running.get(id);
        if (node == null) {
            throw new IllegalStateException(id + " is not running");
        }
        return node;
    }

    Collection<RaftNode> runningNodes() {
        return running.values();
    }

    List<NodeId> crashedNodes() {
        return ids.stream().filter(id -> !running.containsKey(id)).toList();
    }

    InMemoryMetaStore metaStore(NodeId id) {
        return metaStores.get(id);
    }

    TrackingLog log(NodeId id) {
        return logs.get(id);
    }

    /** The state machine of the node's current run (a restart gets a fresh one). */
    RecordingStateMachine stateMachine(NodeId id) {
        return stateMachines.get(id);
    }

    /** Proposes on whichever running node is leader of the highest term; fails if there is none. */
    CompletableFuture<CommitResult> propose(String command) {
        RaftNode leader = runningNodes().stream().filter(n -> n.role() == Role.LEADER)
                .max(java.util.Comparator.comparingLong(RaftNode::currentTerm))
                .orElseThrow(() -> new IllegalStateException("no leader to propose to"));
        return leader.propose(Bytes.ofUtf8(command));
    }

    /** Runs until every running node has applied at least up to {@code index}. */
    void awaitAppliedEverywhere(long index, Duration max) {
        boolean ok = sim.runUntil(() -> runningNodes().stream().allMatch(n -> n.lastApplied() >= index), max);
        if (!ok) {
            throw new AssertionError("index " + index + " not applied on every node within " + max
                    + " (seed " + sim.seed() + "): " + runningNodes());
        }
    }

    List<RaftNode> leaders() {
        return running.values().stream().filter(n -> n.role() == Role.LEADER).toList();
    }

    void run(Duration d) {
        sim.runFor(d);
    }

    /**
     * The group has converged when, among {@code members} (all running): exactly one is leader,
     * and every other member is a follower in the same term that knows who the leader is.
     */
    Optional<RaftNode> convergedLeader(Collection<NodeId> members) {
        RaftNode leader = null;
        for (NodeId id : members) {
            RaftNode n = running.get(id);
            if (n == null) {
                return Optional.empty();
            }
            if (n.role() == Role.LEADER) {
                if (leader != null) {
                    return Optional.empty();
                }
                leader = n;
            }
        }
        if (leader == null) {
            return Optional.empty();
        }
        for (NodeId id : members) {
            RaftNode n = running.get(id);
            if (n != leader && (n.role() != Role.FOLLOWER || n.currentTerm() != leader.currentTerm()
                    || !n.leaderId().equals(Optional.of(leader.id())))) {
                return Optional.empty();
            }
        }
        return Optional.of(leader);
    }

    /** Runs until {@code members} converge on one leader, or fails after {@code max} of virtual time. */
    RaftNode awaitLeader(Collection<NodeId> members, Duration max) {
        boolean ok = sim.runUntil(() -> convergedLeader(members).isPresent(), max);
        if (!ok) {
            throw new AssertionError("no converged leader among " + members + " within " + max
                    + " (seed " + sim.seed() + "); nodes: " + running.values()
                    + "\nrecent trace:\n" + String.join("\n", sim.recentTrace()));
        }
        return convergedLeader(members).orElseThrow();
    }

    RaftNode awaitLeader(Duration max) {
        return awaitLeader(ids, max);
    }
}
