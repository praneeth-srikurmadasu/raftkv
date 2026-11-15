package dev.raftkv.server;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.common.EventLoopScheduler;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.SystemClock;
import dev.raftkv.config.ClusterBootstrap;
import dev.raftkv.io.RealDisk;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.Role;
import dev.raftkv.shard.ShardedKVClient;
import dev.raftkv.transport.NioTransport;

/**
 * A real sharded cluster inside one JVM: each node is a {@link RaftHost} with its own
 * {@link NioTransport} on a loopback port and its own data directory, hosting the controller and
 * every data group, wired exactly as {@code Main} wires them (including the bootstrap Join), minus
 * the separate process. Nodes can be stopped and restarted on the same port and directory.
 */
public final class LocalCluster implements AutoCloseable {

    public static final int SHARDS = 16;

    private final Path root;
    private final int groups;
    private final Map<NodeId, InetSocketAddress> addresses = new TreeMap<>();
    private final Map<NodeId, Node> nodes = new TreeMap<>();
    private final RaftConfig config;
    private final ConcurrentLinkedQueue<Throwable> fatal = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();
    private final List<ClientHandle> clients = new ArrayList<>();

    /** One running node; owns its host, transport and bootstrapper. */
    private final class Node {
        final RaftHost host;
        final NioTransport transport;
        final ClusterBootstrap bootstrap;

        Node(NodeId id) {
            host = new RaftHost(id, new RealDisk(), root.resolve("n" + id.value()), SystemClock.INSTANCE, events::add, fatal::add);
            transport = NioTransport.start(id, addresses.get(id), addresses, host);
            host.attach(transport);
            List<NodeId> servers = ids();
            host.addControllerGroup(addresses.keySet(), SHARDS, config);
            for (int gid = 1; gid <= groups; gid++) {
                host.addShardGroup(gid, addresses.keySet(), SHARDS, servers, config);
            }
            bootstrap = new ClusterBootstrap(id, servers, groups, transport, host.router(), SystemClock.INSTANCE, events::add);
            bootstrap.start();
        }

        void stop() {
            bootstrap.close();
            host.close();
            transport.close();
        }
    }

    /** A client wired the way the CLI wires it. */
    private static final class ClientHandle {
        final EventLoopScheduler loop = new EventLoopScheduler("test-client");
        NioTransport transport;
        ShardedKVClient client;

        void close() {
            transport.close();
            loop.close();
        }
    }

    public LocalCluster(int size, int groups) {
        this(size, groups, RaftConfig.defaults());
    }

    /** With a specific Raft configuration on every group (benchmarks compare settings). */
    public LocalCluster(int size, int groups, RaftConfig config) {
        this.groups = groups;
        this.config = config;
        root = RealNet.tempDir("raftkv-cluster");
        for (int i = 1; i <= size; i++) {
            addresses.put(NodeId.of(i), RealNet.freeLoopbackAddress());
        }
        for (NodeId id : addresses.keySet()) {
            start(id);
        }
    }

    public Map<NodeId, InetSocketAddress> addresses() {
        return addresses;
    }

    public List<NodeId> ids() {
        return new ArrayList<>(addresses.keySet());
    }

    public int groups() {
        return groups;
    }

    public void start(NodeId id) {
        if (nodes.containsKey(id)) {
            throw new IllegalStateException(id + " is running");
        }
        nodes.put(id, new Node(id));
    }

    /** Stops a node as a clean shutdown would (its files stay). */
    public void stop(NodeId id) {
        Node node = nodes.remove(id);
        if (node != null) {
            node.stop();
        }
    }

    public boolean isRunning(NodeId id) {
        return nodes.containsKey(id);
    }

    /** A new client of the cluster (closed with the cluster). */
    public ShardedKVClient newClient(Duration deadline) {
        ClientHandle h = new ClientHandle();
        ShardedKVClient[] holder = new ShardedKVClient[1];
        h.transport = NioTransport.client(NodeId.of(9_000_000 + clients.size()), addresses, e -> holder[0].onMessage(e));
        ClientConfig d = ClientConfig.defaults();
        holder[0] = new ShardedKVClient(h.transport, h.loop, SystemClock.INSTANCE, new SecureRandom(), ids(),
                new ClientConfig(d.attemptTimeout(), d.backoffBase(), d.backoffMax(), deadline));
        h.client = holder[0];
        clients.add(h);
        return h.client;
    }

    public RaftHost.GroupStatus status(NodeId id, GroupId group) {
        try {
            return nodes.get(id).host.status(group).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("cannot read status of " + group + " on " + id, e);
        }
    }

    /** The leader of {@code group} that every running node agrees on, if they agree. */
    public Optional<NodeId> agreedLeader(GroupId group) {
        NodeId leader = null;
        long term = -1;
        for (NodeId id : nodes.keySet()) {
            RaftHost.GroupStatus s = status(id, group);
            if (s.leader().isEmpty()) {
                return Optional.empty();
            }
            if (leader == null) {
                leader = s.leader().get();
                term = s.term();
            } else if (!leader.equals(s.leader().get()) || term != s.term()) {
                return Optional.empty();
            }
        }
        if (leader == null || !nodes.containsKey(leader) || status(leader, group).role() != Role.LEADER) {
            return Optional.empty();
        }
        return Optional.of(leader);
    }

    public NodeId awaitLeader(GroupId group) {
        NodeId[] found = new NodeId[1];
        RealNet.await("a leader of " + group + " all running nodes agree on", 20_000, () -> {
            found[0] = agreedLeader(group).orElse(null);
            return found[0] != null;
        });
        return found[0];
    }

    /** Waits until, in every group, every running node has applied the same, maximal prefix of the log. */
    public void awaitConverged() {
        RealNet.await("all running nodes applied the same log in every group", 30_000, () -> {
            for (int gid = 0; gid <= groups; gid++) {
                long applied = -1;
                for (NodeId id : nodes.keySet()) {
                    RaftHost.GroupStatus s = status(id, GroupId.of(gid));
                    if (s.lastApplied() != s.lastLogIndex() || (applied != -1 && applied != s.lastApplied())) {
                        return false;
                    }
                    applied = s.lastApplied();
                }
            }
            return true;
        });
    }

    /** Keys a node stores in the shards its data groups serve, summed over the groups. */
    public int keysOn(NodeId id) {
        int total = 0;
        for (int gid = 1; gid <= groups; gid++) {
            total += status(id, GroupId.of(gid)).keys();
        }
        return total;
    }

    /** Errors that would have stopped a real process; tests assert this stays empty. */
    public List<Throwable> fatalErrors() {
        return List.copyOf(fatal);
    }

    public List<String> events() {
        return List.copyOf(events);
    }

    @Override
    public void close() {
        for (ClientHandle h : clients) {
            h.close();
        }
        for (NodeId id : List.copyOf(nodes.keySet())) {
            stop(id);
        }
        RealNet.deleteQuietly(root);
    }
}
