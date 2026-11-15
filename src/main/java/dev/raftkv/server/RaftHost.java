package dev.raftkv.server;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import dev.raftkv.common.Clock;
import dev.raftkv.common.EventLoopScheduler;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Tracer;
import dev.raftkv.io.Disk;
import dev.raftkv.kv.KVStateMachine;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.Role;
import dev.raftkv.raft.log.SegmentedFileLog;
import dev.raftkv.raft.storage.FileMetaStore;
import dev.raftkv.raft.storage.FileSnapshotStore;
import dev.raftkv.shard.ControllerStateMachine;
import dev.raftkv.shard.ShardKVStateMachine;
import dev.raftkv.shard.ShardStatus;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.MessageHandler;
import dev.raftkv.transport.Transport;

/**
 * Everything one server process runs (SPEC §3.1, §6.2): any number of Raft groups sharing one
 * {@link Transport}. Each group has its own event loop thread, its own directory with log, meta
 * file and snapshot, and its own state machine. Incoming messages go through a
 * {@link MessageRouter}: each one is handed to its group's event loop, so the Raft code stays
 * single-threaded exactly as in the simulator.
 *
 * <p>Typical use: {@code new RaftHost(...)}, start a transport with the host as its handler,
 * {@link #attach} it, then add groups.
 */
public final class RaftHost implements MessageHandler, AutoCloseable {

    /** Segment files roll at 64 MiB (SPEC §3.7). */
    public static final long SEGMENT_BYTES = 64L * 1024 * 1024;
    /** Sessions expire after an hour of log time. */
    public static final Duration SESSION_TIMEOUT = Duration.ofHours(1);

    private final NodeId self;
    private final Disk disk;
    private final Path dataDir;
    private final Clock clock;
    private final Tracer tracer;
    private final Consumer<Throwable> onFatal;
    private final MessageRouter router = new MessageRouter();
    private final Map<GroupId, OwnedLoop> loops = new ConcurrentHashMap<>();

    /** A group's event loop thread, created and owned here (closed in {@link RaftHost#close}). */
    private static final class OwnedLoop {
        final EventLoopScheduler loop;

        OwnedLoop(String name, Consumer<Throwable> onFatal) {
            this.loop = new EventLoopScheduler(name, onFatal);
        }
    }
    private volatile Transport transport;

    /**
     * A snapshot of one group's state, for status commands and tests.
     *
     * @param leader      the leader this node knows of, possibly itself
     * @param keys        keys stored in this replica (for a data group: in the shards it serves)
     * @param configNum   for a shard data group, the configuration it has applied; for the
     *                    controller, the latest configuration; -1 otherwise
     * @param shardsServing for a data group, how many shards it serves (SERVING or GCING); -1 otherwise
     * @param shardsMoving  for a data group, shards PULLING or BEPULLING; -1 otherwise
     */
    public record GroupStatus(GroupId group, NodeId node, Role role, long term, Optional<NodeId> leader,
                              long commitIndex, long lastApplied, long lastLogIndex, long snapshotIndex, int keys,
                              long configNum, int shardsServing, int shardsMoving) {
    }

    /**
     * @param dataDir root data directory; group {@code g} lives in {@code dataDir/group-g}
     * @param onFatal called (on the event loop) if a group hits an unrecoverable error such as a
     *                failed fsync; the process should stop (fail-stop)
     */
    public RaftHost(NodeId self, Disk disk, Path dataDir, Clock clock, Tracer tracer, Consumer<Throwable> onFatal) {
        this.self = Objects.requireNonNull(self, "self");
        this.disk = Objects.requireNonNull(disk, "disk");
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tracer = Objects.requireNonNull(tracer, "tracer");
        this.onFatal = Objects.requireNonNull(onFatal, "onFatal");
    }

    /** Sets the transport used for every group. Must be called once, before adding groups. */
    public void attach(Transport t) {
        if (transport != null) {
            throw new IllegalStateException("transport already attached");
        }
        if (!t.self().equals(self)) {
            throw new IllegalArgumentException("transport is for " + t.self() + ", host is " + self);
        }
        transport = t;
        router.attach(t);
    }

    /** Adds a single, unsharded KV group (Milestone 7 style). */
    public void addKvGroup(GroupId group, Set<NodeId> voters, RaftConfig config) {
        addGroup(group, voters, config, p -> Replicas.kv(p, SESSION_TIMEOUT));
    }

    /** Adds this node's replica of the shard controller (group 0). */
    public void addControllerGroup(Set<NodeId> voters, int numShards, RaftConfig config) {
        addGroup(GroupId.CONTROLLER, voters, config, p -> Replicas.controller(p, numShards, SESSION_TIMEOUT));
    }

    /** Adds this node's replica of data group {@code gid} of the sharded cluster. */
    public void addShardGroup(int gid, Set<NodeId> voters, int numShards, List<NodeId> controllerServers, RaftConfig config) {
        addGroup(GroupId.of(gid), voters, config, p -> Replicas.shard(p, numShards, SESSION_TIMEOUT, controllerServers, router));
    }

    /**
     * Opens (or creates) the group's storage, recovers it, and starts its replica. Returns when
     * the replica is running.
     */
    private void addGroup(GroupId group, Set<NodeId> voters, RaftConfig config, Function<Replicas.Parts, GroupReplica> build) {
        Transport t = Objects.requireNonNull(transport, "attach a transport first");
        if (router.replica(group).isPresent()) {
            throw new IllegalStateException(group + " already added");
        }
        Path dir = dataDir.resolve("group-" + group.value());
        disk.createDirectories(dir);
        OwnedLoop owned = new OwnedLoop("raftkv-" + self + "-" + group, onFatal);
        EventLoopScheduler loop = owned.loop;
        SegmentedFileLog log = null;
        try {
            log = SegmentedFileLog.open(disk, dir.resolve("log"), SEGMENT_BYTES);
            Replicas.Parts parts = new Replicas.Parts(self, group, voters, config, loop, clock, new Random(), t, log,
                    new FileMetaStore(disk, dir), new FileSnapshotStore(disk, dir), tracer);
            GroupReplica replica = build.apply(parts);
            router.add(replica); // messages that arrive before start() are ignored (the node isn't running)
            loops.put(group, owned);
            onLoop(loop, replica::start).join();
        } catch (RuntimeException e) {
            router.remove(group);
            loops.remove(group);
            owned.loop.close();
            if (log != null) {
                log.close();
            }
            throw e;
        }
    }

    /** Routes one incoming message (called on the transport thread). */
    @Override
    public void onMessage(Envelope envelope) {
        router.onMessage(envelope);
    }

    /** The router, e.g. to register an in-process client for its replies. */
    public MessageRouter router() {
        return router;
    }

    /** The current state of {@code group} on this node (read on its event loop). */
    public CompletableFuture<GroupStatus> status(GroupId group) {
        Optional<GroupReplica> r = router.replica(group);
        if (r.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("no " + group + " on " + self));
        }
        GroupReplica replica = r.get();
        CompletableFuture<GroupStatus> result = new CompletableFuture<>();
        replica.scheduler().execute(() -> {
            try {
                result.complete(describe(replica));
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    /**
     * Every number the metrics endpoint publishes for {@code group} on this node, read on the
     * group's event loop (SPEC §8). Keys are stable metric names; values are numbers, strings or
     * lists of shard numbers.
     */
    public CompletableFuture<Map<String, Object>> metrics(GroupId group) {
        Optional<GroupReplica> r = router.replica(group);
        if (r.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("no " + group + " on " + self));
        }
        GroupReplica replica = r.get();
        CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();
        replica.scheduler().execute(() -> {
            try {
                RaftNode n = replica.node();
                Map<String, Object> m = new java.util.LinkedHashMap<>();
                m.put("group", group.value());
                m.put("role", n.role().toString());
                m.put("term", n.currentTerm());
                m.put("leader", n.leaderId().map(NodeId::value).orElse(-1));
                m.put("commit_index", n.commitIndex());
                m.put("last_applied", n.lastApplied());
                m.put("last_log_index", n.log().lastIndex());
                m.put("snapshot_index", n.log().snapshotIndex());
                m.put("voters", n.membership().voters().stream().map(NodeId::value).toList());
                m.put("learners", n.membership().learners().stream().map(NodeId::value).toList());
                m.put("elections_started", n.electionsStarted());
                m.put("pre_votes_started", n.preVotesStarted());
                m.put("log_syncs", n.logSyncs());
                m.put("snapshots_taken", n.snapshotsTaken());
                m.put("snapshots_installed", n.snapshotsInstalled());
                m.put("pending_proposals", n.pendingProposals());
                m.put("pending_reads", n.pendingReads());
                m.put("holds_lease", n.holdsLease() ? 1 : 0);
                if (replica.stateMachine() instanceof ShardKVStateMachine s) {
                    m.put("config_num", s.current().num());
                    m.put("shards_serving", s.shardsIn(ShardStatus.SERVING));
                    m.put("shards_gcing", s.shardsIn(ShardStatus.GCING));
                    m.put("shards_pulling", s.shardsIn(ShardStatus.PULLING));
                    m.put("shards_bepulling", s.shardsIn(ShardStatus.BEPULLING));
                } else if (replica.stateMachine() instanceof ControllerStateMachine c) {
                    m.put("config_num", c.latest().num());
                }
                result.complete(m);
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    private GroupStatus describe(GroupReplica replica) {
        RaftNode n = replica.node();
        int keys = 0;
        long configNum = -1;
        int serving = -1;
        int moving = -1;
        switch (replica.stateMachine()) {
            case KVStateMachine kv -> keys = kv.size();
            case ControllerStateMachine c -> configNum = c.latest().num();
            case ShardKVStateMachine s -> {
                configNum = s.current().num();
                serving = s.shardsIn(ShardStatus.SERVING).size() + s.shardsIn(ShardStatus.GCING).size();
                moving = s.shardsIn(ShardStatus.PULLING).size() + s.shardsIn(ShardStatus.BEPULLING).size();
                for (int shard = 0; shard < s.numShards(); shard++) {
                    if (s.status(shard).serves()) {
                        keys += s.shardData(shard).map(d -> d.size()).orElse(0);
                    }
                }
            }
            default -> {
                // unknown state machine: no details
            }
        }
        return new GroupStatus(replica.group(), self, n.role(), n.currentTerm(), n.leaderId(), n.commitIndex(),
                n.lastApplied(), n.log().lastIndex(), n.log().snapshotIndex(), keys, configNum, serving, moving);
    }

    public NodeId self() {
        return self;
    }

    public List<GroupId> groups() {
        return router.replicas().keySet().stream().sorted().toList();
    }

    /** Stops every group: stops its replica, closes its log, stops its event loop. */
    @Override
    public void close() {
        for (Map.Entry<GroupId, GroupReplica> e : router.replicas().entrySet()) {
            GroupReplica replica = e.getValue();
            OwnedLoop owned = loops.remove(e.getKey());
            if (owned == null) {
                continue;
            }
            EventLoopScheduler loop = owned.loop;
            try {
                onLoop(loop, () -> {
                    replica.stop();
                    replica.log().close();
                }).join();
            } catch (RuntimeException ex) {
                tracer.trace(self + "/" + e.getKey() + " error while stopping: " + ex);
            }
            loop.close();
            router.remove(e.getKey());
        }
    }

    private static CompletableFuture<Void> onLoop(EventLoopScheduler loop, Runnable task) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        loop.execute(() -> {
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
