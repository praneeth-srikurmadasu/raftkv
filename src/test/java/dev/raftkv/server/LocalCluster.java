package dev.raftkv.server;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import dev.raftkv.Main;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.SystemClock;
import dev.raftkv.io.RealDisk;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.Role;
import dev.raftkv.transport.NioTransport;

/**
 * A real cluster inside one JVM: each node is a {@link RaftHost} with its own
 * {@link NioTransport} on a loopback port and its own data directory, exactly as {@link Main}
 * wires them, minus the separate process. Nodes can be stopped and restarted on the same port
 * and directory.
 */
public final class LocalCluster implements AutoCloseable {

    private final Path root;
    private final Map<NodeId, InetSocketAddress> addresses = new TreeMap<>();
    private final Map<NodeId, Node> nodes = new TreeMap<>();
    private final ConcurrentLinkedQueue<Throwable> fatal = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();

    /** One running node; owns its host and transport. */
    private final class Node {
        final RaftHost host;
        final NioTransport transport;

        Node(NodeId id) {
            host = new RaftHost(id, new RealDisk(), root.resolve("n" + id.value()), SystemClock.INSTANCE, events::add, fatal::add);
            transport = NioTransport.start(id, addresses.get(id), addresses, host);
            host.attach(transport);
            host.addGroup(Main.DATA_GROUP, addresses.keySet(), RaftConfig.defaults());
        }

        void stop() {
            host.close();
            transport.close();
        }
    }

    public LocalCluster(int size) {
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

    public RaftHost.GroupStatus status(NodeId id) {
        try {
            return nodes.get(id).host.status(Main.DATA_GROUP).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("cannot read status of " + id, e);
        }
    }

    /** The leader that every running node agrees on, if they agree. */
    public Optional<NodeId> agreedLeader() {
        NodeId leader = null;
        long term = -1;
        for (NodeId id : nodes.keySet()) {
            RaftHost.GroupStatus s = status(id);
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
        if (leader == null || !nodes.containsKey(leader) || status(leader).role() != Role.LEADER) {
            return Optional.empty();
        }
        return Optional.of(leader);
    }

    public NodeId awaitLeader() {
        NodeId[] found = new NodeId[1];
        RealNet.await("a leader all running nodes agree on", 20_000, () -> {
            found[0] = agreedLeader().orElse(null);
            return found[0] != null;
        });
        return found[0];
    }

    /** Waits until every running node has applied the same, maximal prefix of the log. */
    public void awaitConverged() {
        RealNet.await("all running nodes applied the same log", 30_000, () -> {
            long applied = -1;
            for (NodeId id : nodes.keySet()) {
                RaftHost.GroupStatus s = status(id);
                if (s.lastApplied() != s.lastLogIndex()) {
                    return false;
                }
                if (applied != -1 && applied != s.lastApplied()) {
                    return false;
                }
                applied = s.lastApplied();
            }
            return true;
        });
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
        for (NodeId id : List.copyOf(nodes.keySet())) {
            stop(id);
        }
        RealNet.deleteQuietly(root);
    }
}
