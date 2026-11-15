package dev.raftkv.shard;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.io.sim.SimulatedDisk;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.Role;
import dev.raftkv.raft.log.InMemoryLog;
import dev.raftkv.raft.log.RaftLog;
import dev.raftkv.raft.log.SegmentedFileLog;
import dev.raftkv.raft.storage.FileMetaStore;
import dev.raftkv.raft.storage.FileSnapshotStore;
import dev.raftkv.raft.storage.InMemoryMetaStore;
import dev.raftkv.raft.storage.InMemorySnapshotStore;
import dev.raftkv.raft.storage.MetaStore;
import dev.raftkv.raft.storage.PersistentState;
import dev.raftkv.raft.storage.SnapshotStore;
import dev.raftkv.server.GroupReplica;
import dev.raftkv.server.MessageRouter;
import dev.raftkv.server.Replicas;
import dev.raftkv.sim.Simulation;
import dev.raftkv.transport.Transport;
import dev.raftkv.transport.sim.SimulatedNetwork;

/**
 * A whole sharded RaftKV cluster in one {@link Simulation}: the controller group (0) and data
 * groups 1..G, each a Raft group of three replicas, plus clients. Every node runs the production
 * {@link MessageRouter} and {@link Replicas}, so routing, polling and migration are the real code.
 *
 * <p>Two layouts: <em>separate</em> (controller on n1-n3, group g on n(10g+1)..n(10g+3)) and
 * <em>co-located</em> (every group on n1-n3, one process hosting a replica of each group, like the
 * real deployment). Crashing a node crashes all of its replicas; with disks, unsynced writes may be
 * lost or torn. {@link ShardInvariants} run after every event.
 */
final class ShardedCluster {

    enum Storage { MEMORY, DISK }

    static final Duration SESSION_TIMEOUT = Duration.ofHours(1);
    static final long SEGMENT_BYTES = 4096;

    final Simulation sim;
    final SimulatedNetwork net;
    final RaftConfig config;
    final int numShards;
    final Storage storage;
    final List<NodeId> controllerNodes;
    final Map<Integer, List<NodeId>> groupNodes = new TreeMap<>();
    final List<NodeId> allNodes;
    final ShardInvariants invariants;

    private final Map<NodeId, Transport> transports = new TreeMap<>();
    private final Map<NodeId, MessageRouter> routers = new TreeMap<>();
    private final Map<NodeId, Map<GroupId, GroupReplica>> running = new TreeMap<>();
    private final Map<NodeId, SimulatedDisk> disks = new TreeMap<>();
    private final Map<String, MetaStore> metaStores = new TreeMap<>();
    private final Map<String, SnapshotStore> snapshotStores = new TreeMap<>();
    private final Map<String, RaftLog> logs = new TreeMap<>();
    private int clientCount;

    ShardedCluster(long seed, int groups, int numShards, RaftConfig config, Storage storage, boolean colocated) {
        this.sim = new Simulation(seed);
        if (Boolean.getBoolean("raftkv.trace")) {
            sim.echoTraceTo(System.out::println); // -Draftkv.trace=true with -Draftkv.seed=S prints every event
        }
        this.net = new SimulatedNetwork(sim);
        this.config = config;
        this.numShards = numShards;
        this.storage = storage;
        this.controllerNodes = List.of(NodeId.of(1), NodeId.of(2), NodeId.of(3));
        for (int gid = 1; gid <= groups; gid++) {
            groupNodes.put(gid, colocated ? controllerNodes
                    : List.of(NodeId.of(10 * gid + 1), NodeId.of(10 * gid + 2), NodeId.of(10 * gid + 3)));
        }
        TreeSet<NodeId> all = new TreeSet<>(controllerNodes);
        groupNodes.values().forEach(all::addAll);
        this.allNodes = List.copyOf(all);
        for (NodeId id : allNodes) {
            if (storage == Storage.DISK) {
                disks.put(id, new SimulatedDisk(sim.forkRandom()));
            }
            transports.put(id, net.register(id, envelope -> {
                MessageRouter router = routers.get(id);
                if (router != null) {
                    router.onMessage(envelope);
                }
            }));
        }
        this.invariants = new ShardInvariants(this);
        sim.addInvariant(invariants);
    }

    /** The groups (0 = controller) a node hosts. */
    List<GroupId> groupsOn(NodeId id) {
        List<GroupId> out = new ArrayList<>();
        if (controllerNodes.contains(id)) {
            out.add(GroupId.CONTROLLER);
        }
        for (Map.Entry<Integer, List<NodeId>> e : groupNodes.entrySet()) {
            if (e.getValue().contains(id)) {
                out.add(GroupId.of(e.getKey()));
            }
        }
        return out;
    }

    List<NodeId> members(GroupId group) {
        return group.equals(GroupId.CONTROLLER) ? controllerNodes : groupNodes.get(group.value());
    }

    void startAll() {
        allNodes.forEach(this::start);
    }

    private void start(NodeId id) {
        MessageRouter router = new MessageRouter();
        router.attach(transports.get(id));
        routers.put(id, router);
        Map<GroupId, GroupReplica> replicas = new TreeMap<>();
        for (GroupId g : groupsOn(id)) {
            String key = id + "/" + g;
            if (!metaStores.containsKey(key) || storage == Storage.DISK) {
                openStorage(id, g, key);
            }
            Replicas.Parts parts = new Replicas.Parts(id, g, new TreeSet<>(members(g)), config, sim, sim.clock(),
                    sim.forkRandom(), transports.get(id), logs.get(key), metaStores.get(key), snapshotStores.get(key), sim);
            GroupReplica r = g.equals(GroupId.CONTROLLER)
                    ? Replicas.controller(parts, numShards, SESSION_TIMEOUT)
                    : Replicas.shard(parts, numShards, SESSION_TIMEOUT, controllerNodes, router);
            if (r.stateMachine() instanceof ShardKVStateMachine s) {
                invariants.watch(s);
            }
            router.add(r);
            replicas.put(g, r);
        }
        running.put(id, replicas);
        for (GroupReplica r : replicas.values()) {
            r.start();
        }
    }

    private void openStorage(NodeId id, GroupId g, String key) {
        if (storage == Storage.MEMORY) {
            metaStores.put(key, new InMemoryMetaStore(PersistentState.INITIAL));
            snapshotStores.put(key, new InMemorySnapshotStore());
            logs.put(key, new InMemoryLog());
        } else {
            SimulatedDisk disk = disks.get(id);
            Path dir = Path.of("/data/n" + id.value() + "/group-" + g.value());
            metaStores.put(key, new FileMetaStore(disk, dir));
            snapshotStores.put(key, new FileSnapshotStore(disk, dir));
            logs.put(key, SegmentedFileLog.open(disk, dir.resolve("log"), SEGMENT_BYTES));
        }
    }

    /** Kills every replica on the node; with disks, unsynced data may be lost or torn. */
    void crash(NodeId id) {
        Map<GroupId, GroupReplica> replicas = running.remove(id);
        if (replicas == null) {
            throw new IllegalStateException(id + " is not running");
        }
        replicas.values().forEach(GroupReplica::stop);
        routers.remove(id);
        net.setNodeUp(id, false);
        if (storage == Storage.DISK) {
            disks.get(id).crash(SimulatedDisk.CrashMode.RANDOM);
        }
    }

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

    List<NodeId> crashedNodes() {
        return allNodes.stream().filter(id -> !running.containsKey(id)).toList();
    }

    /** Running replicas of a group. */
    List<GroupReplica> replicas(GroupId group) {
        List<GroupReplica> out = new ArrayList<>();
        for (Map<GroupId, GroupReplica> m : running.values()) {
            GroupReplica r = m.get(group);
            if (r != null) {
                out.add(r);
            }
        }
        return out;
    }

    Collection<Map<GroupId, GroupReplica>> allRunning() {
        return running.values();
    }

    Optional<GroupReplica> replica(NodeId id, GroupId group) {
        Map<GroupId, GroupReplica> m = running.get(id);
        return m == null ? Optional.empty() : Optional.ofNullable(m.get(group));
    }

    Optional<RaftNode> leader(GroupId group) {
        return replicas(group).stream().map(GroupReplica::node).filter(n -> n.role() == Role.LEADER)
                .max(java.util.Comparator.comparingLong(RaftNode::currentTerm));
    }

    /** A new sharded client (node 1000 + n), with the simulation as its event loop. */
    ShardedKVClient newClient(ClientConfig clientConfig) {
        NodeId node = NodeId.of(1000 + clientCount++);
        ShardedKVClient[] holder = new ShardedKVClient[1];
        Transport t = net.register(node, e -> holder[0].onMessage(e));
        holder[0] = new ShardedKVClient(t, sim, sim.clock(), sim.forkRandom(), controllerNodes, clientConfig);
        return holder[0];
    }

    /** An admin client of the controller (node 2000 + n). */
    ControllerClient newAdmin(ClientConfig clientConfig) {
        NodeId node = NodeId.of(2000 + clientCount++);
        ControllerClient[] holder = new ControllerClient[1];
        Transport t = net.register(node, e -> holder[0].onMessage(e));
        holder[0] = new ControllerClient(t, sim, sim.clock(), sim.forkRandom(), controllerNodes, clientConfig);
        return holder[0];
    }

    /** {@code gid -> servers} for a Join of the given groups. */
    java.util.SortedMap<Integer, List<NodeId>> groupsFor(int... gids) {
        java.util.SortedMap<Integer, List<NodeId>> m = new TreeMap<>();
        for (int gid : gids) {
            m.put(gid, groupNodes.get(gid));
        }
        return m;
    }

    <T> T await(CompletableFuture<T> future, Duration max) {
        sim.runUntil(future::isDone, max);
        if (!future.isDone()) {
            throw new AssertionError("did not complete within " + max + " (seed " + sim.seed() + ")");
        }
        return future.join();
    }

    void run(Duration d) {
        sim.runFor(d);
    }

    /** The latest configuration according to a running controller replica that has applied the most. */
    ShardConfig controllerConfig() {
        return replicas(GroupId.CONTROLLER).stream()
                .max(java.util.Comparator.comparingLong(r -> r.node().lastApplied()))
                .map(r -> ((ControllerStateMachine) r.stateMachine()).latest())
                .orElseThrow(() -> new IllegalStateException("no controller replica running"));
    }

    /**
     * Quiescence: every data group's replicas have applied the controller's latest configuration,
     * every shard is settled (SERVING or NOT_OWNED), and replicas of each group agree on their state.
     */
    boolean quiescent() {
        ShardConfig latest = controllerConfig();
        for (int gid : groupNodes.keySet()) {
            List<GroupReplica> rs = replicas(GroupId.of(gid));
            if (rs.size() != groupNodes.get(gid).size()) {
                return false;
            }
            long applied = -1;
            for (GroupReplica r : rs) {
                ShardKVStateMachine s = (ShardKVStateMachine) r.stateMachine();
                if (s.current().num() != latest.num() || !s.allSettled()) {
                    return false;
                }
                if (applied != -1 && r.node().lastApplied() != applied) {
                    return false;
                }
                applied = r.node().lastApplied();
            }
            if (rs.stream().noneMatch(r -> r.node().role() == Role.LEADER && r.node().commitIndex() == r.node().lastApplied())) {
                return false;
            }
        }
        return true;
    }

    void awaitQuiescent(Duration max) {
        if (!sim.runUntil(this::quiescent, max)) {
            StringBuilder sb = new StringBuilder("cluster did not settle within " + max + " (seed " + sim.seed() + "): latest "
                    + controllerConfig());
            for (int gid : groupNodes.keySet()) {
                for (GroupReplica r : replicas(GroupId.of(gid))) {
                    ShardKVStateMachine s = (ShardKVStateMachine) r.stateMachine();
                    sb.append("\n  g").append(gid).append(' ').append(r.node()).append(" config ").append(s.current().num())
                            .append(" pulling ").append(s.shardsIn(ShardStatus.PULLING))
                            .append(" bepulling ").append(s.shardsIn(ShardStatus.BEPULLING))
                            .append(" gcing ").append(s.shardsIn(ShardStatus.GCING));
                }
            }
            throw new AssertionError(sb.toString());
        }
    }

    /**
     * Checks "No lost data" at quiescence (SPEC §6.10): each shard's data lives in exactly the group
     * the latest configuration assigns it to, identically on all its replicas, and nowhere else.
     */
    void checkSettledOwnership() {
        ShardConfig latest = controllerConfig();
        for (int shard = 0; shard < numShards; shard++) {
            int owner = latest.gidOf(shard);
            for (int gid : groupNodes.keySet()) {
                Long digest = null;
                for (GroupReplica r : replicas(GroupId.of(gid))) {
                    ShardKVStateMachine s = (ShardKVStateMachine) r.stateMachine();
                    ShardStatus st = s.status(shard);
                    if (gid == owner) {
                        if (st != ShardStatus.SERVING) {
                            throw new AssertionError("shard " + shard + " should be SERVING in g" + gid + " but is " + st);
                        }
                        long d = s.shardData(shard).map(x -> (long) x.entries().hashCode()).orElse(0L);
                        if (digest != null && digest != d) {
                            throw new AssertionError("replicas of g" + gid + " hold different data for shard " + shard);
                        }
                        digest = d;
                    } else if (st != ShardStatus.NOT_OWNED || s.shardData(shard).isPresent()) {
                        throw new AssertionError("g" + gid + " still holds shard " + shard + " (" + st + ") owned by g" + owner);
                    }
                }
            }
        }
    }

    Set<Integer> gids() {
        return groupNodes.keySet();
    }
}
