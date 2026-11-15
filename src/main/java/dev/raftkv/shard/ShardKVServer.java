package dev.raftkv.shard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Cancellable;
import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Scheduler;
import dev.raftkv.kv.KVCodec;
import dev.raftkv.kv.KVCommand;
import dev.raftkv.kv.SessionCommand;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.protocol.DeleteShard;
import dev.raftkv.protocol.DeleteShardReply;
import dev.raftkv.protocol.FetchShard;
import dev.raftkv.protocol.FetchShardReply;
import dev.raftkv.raft.Bug;
import dev.raftkv.raft.NotLeaderException;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.Role;
import dev.raftkv.transport.ClientRegistry;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.MessageHandler;
import dev.raftkv.transport.Transport;

/**
 * Everything a replica of a data group does besides Raft itself (SPEC §6.5, §6.6). All of it runs
 * on the group's event loop.
 *
 * <ul>
 *   <li><b>Client requests:</b> checked against the current shard statuses on arrival (to save a
 *       log entry), proposed, and answered from the outcome; the state machine checks ownership
 *       again when the entry is applied.</li>
 *   <li><b>Config poller</b> (leader only, every {@link #POLL_INTERVAL}): when every shard has
 *       settled, asks the controller for configuration {@code current + 1} and proposes it. Never
 *       skips a configuration.</li>
 *   <li><b>Migrator</b> (leader only, every {@link #MIGRATE_INTERVAL}): for PULLING shards, sends
 *       FetchShard to the previous owner's servers and proposes InsertShards with the reply; for
 *       GCING shards, sends DeleteShard and, once confirmed, proposes GcDone.</li>
 *   <li><b>Serving other groups</b> (any replica): answers FetchShard from frozen BEPULLING data;
 *       DeleteShard is proposed through the log by the leader and confirmed once applied.</li>
 * </ul>
 * Everything the poller and migrator decide is derived from the replicated shard statuses, so a
 * leader that crashes mid-migration leaves nothing behind: the next leader picks up from the state.
 */
public final class ShardKVServer implements MessageHandler {

    public static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    public static final Duration MIGRATE_INTERVAL = Duration.ofMillis(50);

    private final RaftNode raft;
    private final ShardKVStateMachine sm;
    private final Transport transport;
    private final Clock clock;
    private final Scheduler scheduler;
    private final GroupId group;
    private final ControllerClient controller;
    private final Set<Bug> bugs;

    private Cancellable pollTimer;
    private Cancellable migrateTimer;
    private boolean queryInFlight;
    /** Proposals in flight, so the same shard op isn't proposed again on every tick. */
    private final Set<String> proposing = new HashSet<>();
    private long requests;

    public ShardKVServer(RaftNode raft, ShardKVStateMachine sm, Transport transport, Clock clock, Scheduler scheduler,
                         Random random, List<NodeId> controllerServers, ClientRegistry registry) {
        this.raft = Objects.requireNonNull(raft, "raft");
        this.sm = Objects.requireNonNull(sm, "sm");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.group = raft.group();
        if (group.value() != sm.gid()) {
            throw new IllegalArgumentException("state machine of group " + sm.gid() + " in Raft group " + group);
        }
        this.bugs = raft.config().bugs();
        ClientConfig cc = new ClientConfig(Duration.ofMillis(300), Duration.ofMillis(20), Duration.ofMillis(500),
                Duration.ofSeconds(2));
        this.controller = new ControllerClient(transport, scheduler, clock, random, controllerServers, cc);
        registry.register(controller.clientId(), controller::onMessage);
    }

    /** Starts the poller and migrator loops (they act only while this replica is leader). */
    public void start() {
        pollTimer = scheduler.schedule(POLL_INTERVAL, this::poll);
        migrateTimer = scheduler.schedule(MIGRATE_INTERVAL, this::migrate);
    }

    public void stop() {
        if (pollTimer != null) {
            pollTimer.cancel();
        }
        if (migrateTimer != null) {
            migrateTimer.cancel();
        }
        proposing.clear();
        queryInFlight = false;
    }

    @Override
    public void onMessage(Envelope envelope) {
        switch (envelope.message()) {
            case ClientRequest r -> onClientRequest(envelope, r);
            case FetchShard f -> onFetchShard(envelope, f);
            case FetchShardReply f -> onFetchShardReply(f);
            case DeleteShard d -> onDeleteShard(envelope, d);
            case DeleteShardReply d -> onDeleteShardReply(d);
            default -> {
                // Raft messages go to the RaftNode; client replies to the registered clients.
            }
        }
    }

    // ------------------------------------------------------------------ clients

    private void onClientRequest(Envelope envelope, ClientRequest request) {
        requests++;
        if (raft.role() != Role.LEADER) {
            reply(envelope, ClientReply.to(request, ClientStatus.NOT_LEADER, raft.leaderId().orElse(null), Bytes.EMPTY));
            return;
        }
        KVCommand command;
        try {
            command = KVCodec.decodeCommand(request.command());
        } catch (CodecException e) {
            reply(envelope, ClientReply.to(request, ClientStatus.INVALID_REQUEST, null, Bytes.EMPTY));
            return;
        }
        ClientStatus early = sm.receiveCheck(command.key());
        if (early != ClientStatus.OK) {
            reply(envelope, ClientReply.to(request, early, null, Bytes.EMPTY));
            return;
        }
        SessionCommand entry = new SessionCommand(request.clientId(), request.sequence(), request.ackedBelow(),
                clock.wallMillis(), request.command());
        raft.propose(KVCodec.encode(entry)).whenComplete((committed, error) -> {
            if (error == null) {
                KVCodec.Outcome outcome = KVCodec.decodeOutcome(committed.result());
                reply(envelope, ClientReply.to(request, outcome.status(), null, outcome.result()));
            } else {
                Throwable cause = error.getCause() != null ? error.getCause() : error;
                NodeId hint = cause instanceof NotLeaderException nle ? nle.leaderHint().orElse(null) : null;
                reply(envelope, ClientReply.to(request, ClientStatus.NOT_LEADER, hint, Bytes.EMPTY));
            }
        });
    }

    private void reply(Envelope request, ClientReply reply) {
        transport.send(request.from(), group, reply);
    }

    // ------------------------------------------------------------------ config poller (leader)

    private void poll() {
        pollTimer = scheduler.schedule(POLL_INTERVAL, this::poll);
        if (!isLeader() || queryInFlight || !sm.allSettled()) {
            return;
        }
        long want = bugs.contains(Bug.SKIP_CONFIG_NUMBERS) ? ControllerCommand.Query.LATEST : sm.current().num() + 1;
        queryInFlight = true;
        controller.query(want).whenComplete((config, error) -> {
            queryInFlight = false;
            if (config == null || !isLeader() || !sm.allSettled()) {
                return;
            }
            long cur = sm.current().num();
            boolean next = bugs.contains(Bug.SKIP_CONFIG_NUMBERS) ? config.num() > cur : config.num() == cur + 1;
            if (next) {
                propose("config " + config.num(), new ShardOp.ConfigChange(config));
            }
        });
    }

    // ------------------------------------------------------------------ migrator (leader)

    private void migrate() {
        migrateTimer = scheduler.schedule(MIGRATE_INTERVAL, this::migrate);
        if (!isLeader()) {
            return;
        }
        long num = sm.current().num();
        ShardConfig prev = sm.previous();
        for (Map.Entry<Integer, List<Integer>> e : byPreviousOwner(sm.shardsIn(ShardStatus.PULLING)).entrySet()) {
            sendToGroup(prev, e.getKey(), new FetchShard(num, group.value(), e.getValue()));
        }
        for (Map.Entry<Integer, List<Integer>> e : byPreviousOwner(sm.shardsIn(ShardStatus.GCING)).entrySet()) {
            sendToGroup(prev, e.getKey(), new DeleteShard(num, group.value(), e.getValue()));
        }
    }

    private Map<Integer, List<Integer>> byPreviousOwner(List<Integer> shards) {
        Map<Integer, List<Integer>> out = new TreeMap<>();
        for (int s : shards) {
            out.computeIfAbsent(sm.previous().gidOf(s), k -> new ArrayList<>()).add(s);
        }
        return out;
    }

    /** Any replica of the other group can answer a FetchShard, and its leader handles DeleteShard: ask them all. */
    private void sendToGroup(ShardConfig config, int gid, dev.raftkv.protocol.Message message) {
        for (NodeId server : config.servers(gid)) {
            transport.send(server, GroupId.of(gid), message);
        }
    }

    private void onFetchShardReply(FetchShardReply reply) {
        if (!isLeader() || !reply.ready() || reply.configNum() != sm.current().num()) {
            return;
        }
        SortedMap<Integer, ShardData> fetched;
        try {
            fetched = ShardOp.decodeShards(reply.shards());
        } catch (CodecException e) {
            return;
        }
        SortedMap<Integer, ShardData> wanted = new TreeMap<>();
        for (Map.Entry<Integer, ShardData> e : fetched.entrySet()) {
            if (e.getKey() >= 0 && e.getKey() < sm.numShards() && sm.status(e.getKey()) == ShardStatus.PULLING) {
                wanted.put(e.getKey(), e.getValue());
            }
        }
        if (wanted.isEmpty()) {
            return;
        }
        if (bugs.contains(Bug.GC_BEFORE_INSERT_COMMITTED)) {
            // The bug: tell the old owner to delete before our copy is safely in our log.
            int owner = sm.previous().gidOf(wanted.firstKey());
            sendToGroup(sm.previous(), owner, new DeleteShard(reply.configNum(), group.value(), List.copyOf(wanted.keySet())));
        }
        propose("insert " + reply.configNum() + " " + wanted.keySet(), new ShardOp.InsertShards(reply.configNum(), wanted));
    }

    private void onDeleteShardReply(DeleteShardReply reply) {
        if (!isLeader() || !reply.done() || reply.configNum() != sm.current().num()) {
            return;
        }
        List<Integer> done = new ArrayList<>();
        for (int s : reply.shards()) {
            if (s >= 0 && s < sm.numShards() && sm.status(s) == ShardStatus.GCING) {
                done.add(s);
            }
        }
        if (!done.isEmpty()) {
            propose("gcdone " + reply.configNum() + " " + done, new ShardOp.GcDone(reply.configNum(), done));
        }
    }

    // ------------------------------------------------------------------ serving other groups

    private void onFetchShard(Envelope envelope, FetchShard request) {
        Optional<SortedMap<Integer, ShardData>> frozen = sm.frozenShards(request.configNum(), request.shards());
        FetchShardReply reply = frozen
                .map(m -> new FetchShardReply(request.configNum(), true, ShardOp.encodeShards(m)))
                .orElseGet(() -> new FetchShardReply(request.configNum(), false, Bytes.EMPTY));
        transport.send(envelope.from(), GroupId.of(request.requesterGroup()), reply);
    }

    private void onDeleteShard(Envelope envelope, DeleteShard request) {
        GroupId requester = GroupId.of(request.requesterGroup());
        if (sm.deleted(request.configNum(), request.shards())) {
            transport.send(envelope.from(), requester, new DeleteShardReply(request.configNum(), true, request.shards()));
            return;
        }
        if (!isLeader() || sm.current().num() != request.configNum()) {
            return; // not ours to decide, or not there yet: the requester keeps asking
        }
        String key = "delete " + request.configNum() + " " + request.shards();
        if (!proposing.add(key)) {
            return;
        }
        raft.propose(ShardOp.encode(new ShardOp.DeleteShards(request.configNum(), request.shards())))
                .whenComplete((r, e) -> {
                    proposing.remove(key);
                    if (e == null && sm.deleted(request.configNum(), request.shards())) {
                        transport.send(envelope.from(), requester,
                                new DeleteShardReply(request.configNum(), true, request.shards()));
                    }
                });
    }

    // ------------------------------------------------------------------ helpers

    private boolean isLeader() {
        return raft.isRunning() && raft.role() == Role.LEADER;
    }

    private void propose(String key, ShardOp op) {
        if (!proposing.add(key)) {
            return;
        }
        raft.propose(ShardOp.encode(op)).whenComplete((r, e) -> proposing.remove(key));
    }

    /** Client requests handled so far (metrics). */
    public long requestsHandled() {
        return requests;
    }

    public ShardKVStateMachine stateMachine() {
        return sm;
    }
}
