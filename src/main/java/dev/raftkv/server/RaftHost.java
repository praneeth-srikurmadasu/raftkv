package dev.raftkv.server;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.Clock;
import dev.raftkv.common.EventLoopScheduler;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Tracer;
import dev.raftkv.io.Disk;
import dev.raftkv.kv.KVServer;
import dev.raftkv.kv.KVStateMachine;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.protocol.RaftMessage;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.Role;
import dev.raftkv.raft.log.RaftLog;
import dev.raftkv.raft.log.SegmentedFileLog;
import dev.raftkv.raft.storage.FileMetaStore;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.MessageHandler;
import dev.raftkv.transport.Transport;

/**
 * Everything one server process runs (SPEC §3.1): a set of Raft groups that share one
 * {@link Transport}. Each group has its own event loop thread, Raft node, log, metadata store,
 * KV state machine and {@link KVServer}.
 *
 * <p>The host is the transport's {@link MessageHandler}: it looks at the group id of each
 * incoming message and hands the message to that group's event loop. Nothing else crosses
 * threads, so the Raft code stays single-threaded exactly as in the simulator. A client
 * request for a group this host does not run gets {@link ClientStatus#WRONG_GROUP}.
 *
 * <p>Typical use: {@code new RaftHost(...)}, start a transport with the host as its handler,
 * {@link #attach} it, then {@link #addGroup} for each group.
 */
public final class RaftHost implements MessageHandler, AutoCloseable {

    /** Segment files roll at 64 MiB (SPEC §6.2). */
    public static final long SEGMENT_BYTES = 64L * 1024 * 1024;

    private final NodeId self;
    private final Disk disk;
    private final Path dataDir;
    private final Clock clock;
    private final Tracer tracer;
    private final Consumer<Throwable> onFatal;
    private final Map<GroupId, Group> groups = new ConcurrentHashMap<>();
    private volatile Transport transport;

    /** One group's parts. Owns its event loop (created here, closed in {@link RaftHost#close}). */
    private static final class Group {
        final GroupId id;
        final EventLoopScheduler loop;
        final KVStateMachine store = new KVStateMachine();
        volatile RaftLog log;          // set once by addGroup, before the node starts
        volatile RaftNode node;
        volatile KVServer server;

        Group(GroupId id, String threadName, Consumer<Throwable> onFatal) {
            this.id = id;
            this.loop = new EventLoopScheduler(threadName, onFatal);
        }
    }

    /**
     * A snapshot of one group's state, for status commands and tests.
     *
     * @param leader the leader this node knows of, possibly itself
     */
    public record GroupStatus(GroupId group, NodeId node, Role role, long term, Optional<NodeId> leader,
                              long commitIndex, long lastApplied, long lastLogIndex, int keys) {
    }

    /**
     * @param dataDir root data directory; group {@code g} lives in {@code dataDir/group-g}
     * @param tracer  receives Raft events (each line already names the node, group, term and role)
     * @param onFatal called (on the event loop) if a group hits an unrecoverable error such as a
     *                failed fsync; the process should stop (fail-stop, SPEC §6.4)
     */
    public RaftHost(NodeId self, Disk disk, Path dataDir, Clock clock, Tracer tracer, Consumer<Throwable> onFatal) {
        this.self = Objects.requireNonNull(self, "self");
        this.disk = Objects.requireNonNull(disk, "disk");
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tracer = Objects.requireNonNull(tracer, "tracer");
        this.onFatal = Objects.requireNonNull(onFatal, "onFatal");
    }

    /** Sets the transport used for every group. Must be called once, before {@link #addGroup}. */
    public void attach(Transport t) {
        if (transport != null) {
            throw new IllegalStateException("transport already attached");
        }
        if (!t.self().equals(self)) {
            throw new IllegalArgumentException("transport is for " + t.self() + ", host is " + self);
        }
        transport = t;
    }

    /**
     * Opens (or creates) the storage of {@code group}, recovers it, and starts its Raft node.
     * Returns when the node is running.
     */
    public void addGroup(GroupId group, Set<NodeId> voters, RaftConfig config) {
        Transport t = Objects.requireNonNull(transport, "attach a transport first");
        if (groups.containsKey(group)) {
            throw new IllegalStateException(group + " already added");
        }
        Path dir = dataDir.resolve("group-" + group.value());
        disk.createDirectories(dir);
        Group g = new Group(group, "raftkv-" + self + "-" + group, onFatal);
        groups.put(group, g); // messages arriving before start() are ignored (see onMessage)
        try {
            FileMetaStore meta = new FileMetaStore(disk, dir);
            g.log = SegmentedFileLog.open(disk, dir.resolve("log"), SEGMENT_BYTES);
            g.node = RaftNode.builder()
                    .self(self)
                    .group(group)
                    .voters(voters)
                    .config(config)
                    .scheduler(g.loop)
                    .clock(clock)
                    .random(new Random())
                    .transport(t)
                    .log(g.log)
                    .metaStore(meta)
                    .stateMachine(g.store)
                    .tracer(tracer)
                    .build();
            g.server = new KVServer(g.node, t, clock);
            onLoop(g, g.node::start).join();
        } catch (RuntimeException e) {
            groups.remove(group);
            g.loop.close();
            if (g.log != null) {
                g.log.close();
            }
            throw e;
        }
    }

    /** Routes one incoming message to its group's event loop (called on the transport thread). */
    @Override
    public void onMessage(Envelope envelope) {
        Group g = groups.get(envelope.group());
        if (g == null) {
            if (envelope.message() instanceof ClientRequest request && transport != null) {
                transport.send(envelope.from(), envelope.group(),
                        new ClientReply(request.sequence(), ClientStatus.WRONG_GROUP, null, Bytes.EMPTY));
            }
            return;
        }
        g.loop.execute(() -> {
            if (g.node == null || !g.node.isRunning()) {
                return;
            }
            if (envelope.message() instanceof RaftMessage) {
                g.node.handle(envelope);
            } else if (envelope.message() instanceof ClientRequest) {
                g.server.handle(envelope);
            }
        });
    }

    /** The current state of {@code group} on this node (read on its event loop). */
    public CompletableFuture<GroupStatus> status(GroupId group) {
        Group g = groups.get(group);
        if (g == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("no " + group + " on " + self));
        }
        CompletableFuture<GroupStatus> result = new CompletableFuture<>();
        g.loop.execute(() -> {
            RaftNode n = g.node;
            result.complete(new GroupStatus(group, self, n.role(), n.currentTerm(), n.leaderId(),
                    n.commitIndex(), n.lastApplied(), n.log().lastIndex(), g.store.size()));
        });
        return result;
    }

    public NodeId self() {
        return self;
    }

    public List<GroupId> groups() {
        return groups.keySet().stream().sorted().toList();
    }

    /** Stops every group: stops the Raft node, closes its log, stops its event loop. */
    @Override
    public void close() {
        for (Group g : groups.values()) {
            try {
                onLoop(g, () -> {
                    g.node.stop();
                    g.log.close();
                }).join();
            } catch (RuntimeException e) {
                tracer.trace(self + "/" + g.id + " error while stopping: " + e);
            }
            g.loop.close();
        }
        groups.clear();
    }

    private static CompletableFuture<Void> onLoop(Group g, Runnable task) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        g.loop.execute(() -> {
            try {
                task.run();
                done.complete(null);
            } catch (RuntimeException e) {
                done.completeExceptionally(e);
                throw e;
            }
        });
        return done;
    }
}
