package dev.raftkv.kv;

import java.util.Objects;
import java.util.function.Consumer;

import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.raft.Bug;
import dev.raftkv.raft.NotLeaderException;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.ReadMode;
import dev.raftkv.raft.Role;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * The client-facing side of one replica (SPEC §4): turns {@link ClientRequest}s into Raft
 * proposals and Raft results into {@link ClientReply}s. It serves any state machine that takes
 * {@link SessionCommand} entries and answers with {@link KVCodec.Outcome}s: the single-group KV
 * store, and the shard controller (with its own command validator).
 *
 * <ul>
 *   <li>Not the leader → reply NOT_LEADER with a hint; the client retries there with the
 *       <em>same</em> sequence number.</li>
 *   <li>Malformed command or oversized key/value → INVALID_REQUEST, without touching the log.</li>
 *   <li>Otherwise wrap the command in a {@link SessionCommand} stamped with the leader's wall
 *       clock and propose it. When it is applied, reply with the state machine's outcome. If
 *       leadership is lost first, reply NOT_LEADER: the entry may still commit later, which is
 *       exactly why retries need session deduplication.</li>
 * </ul>
 * Must be called on the node's event loop, like the {@link RaftNode} it wraps.
 */
public final class KVServer {

    private final RaftNode raft;
    private final Transport transport;
    private final Clock clock;
    private final GroupId group;
    private final KVStateMachine localStore;
    private final Consumer<Bytes> validator;
    private long requests;

    /** Checks a command before it costs a log entry; throws {@link CodecException} if it is invalid. */
    public static final Consumer<Bytes> KV_COMMANDS = KVCodec::decodeCommand;

    public KVServer(RaftNode raft, Transport transport, Clock clock) {
        this(raft, transport, clock, null);
    }

    /**
     * @param localStore this replica's state machine; only used by the deliberately broken read
     *                   path ({@link Bug#READ_FROM_LOCAL_STATE}) in tests, may be null
     */
    public KVServer(RaftNode raft, Transport transport, Clock clock, KVStateMachine localStore) {
        this(raft, transport, clock, localStore, KV_COMMANDS);
    }

    /**
     * @param validator rejects malformed commands (throws {@link CodecException}) before they are proposed
     */
    public KVServer(RaftNode raft, Transport transport, Clock clock, KVStateMachine localStore, Consumer<Bytes> validator) {
        this.validator = Objects.requireNonNull(validator, "validator");
        this.raft = Objects.requireNonNull(raft, "raft");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.group = raft.group();
        this.localStore = localStore;
    }

    /** Handles one message from a client; ignores anything that is not a {@link ClientRequest}. */
    public void handle(Envelope envelope) {
        if (!(envelope.message() instanceof ClientRequest request)) {
            return;
        }
        requests++;
        if (localStore != null && raft.config().has(Bug.READ_FROM_LOCAL_STATE) && answerLocally(envelope, request)) {
            return;
        }
        if (raft.role() != Role.LEADER) {
            reply(envelope, ClientReply.to(request, ClientStatus.NOT_LEADER, raft.leaderId().orElse(null), Bytes.EMPTY));
            return;
        }
        try {
            validator.accept(request.command()); // validate before it costs a log entry
        } catch (CodecException e) {
            reply(envelope, ClientReply.to(request, ClientStatus.INVALID_REQUEST, null, Bytes.EMPTY));
            return;
        }
        if (localStore != null && raft.config().readMode() != ReadMode.LOG && readWithoutLog(envelope, request)) {
            return;
        }
        SessionCommand entry = new SessionCommand(request.clientId(), request.sequence(), request.ackedBelow(),
                clock.wallMillis(), request.command());
        raft.propose(KVCodec.encode(entry)).whenComplete((committed, error) -> {
            if (error == null) {
                KVCodec.Outcome outcome = KVCodec.decodeOutcome(committed.result());
                reply(envelope, ClientReply.to(request, outcome.status(), null, outcome.result()));
            } else {
                // The only way a proposal fails is losing (or never having) leadership.
                Throwable cause = error.getCause() != null ? error.getCause() : error;
                NodeId hint = cause instanceof NotLeaderException nle ? nle.leaderHint().orElse(null) : null;
                reply(envelope, ClientReply.to(request, ClientStatus.NOT_LEADER, hint, Bytes.EMPTY));
            }
        });
    }

    /**
     * GET without a log entry (SPEC §4.4): ReadIndex or lease, then the local state machine, which
     * by then has applied everything committed when the read arrived. Reads need no session
     * entry: they change nothing, so running one twice is harmless. Returns false for non-GETs.
     */
    private boolean readWithoutLog(Envelope envelope, ClientRequest request) {
        KVCommand command;
        try {
            command = KVCodec.decodeCommand(request.command());
        } catch (CodecException e) {
            return false;
        }
        if (!(command instanceof KVCommand.Get get)) {
            return false;
        }
        raft.read(raft.config().readMode()).whenComplete((index, error) -> {
            if (error == null) {
                Bytes result = KVCodec.encode(new KVResult.GetResult(localStore.peek(get.key())));
                reply(envelope, ClientReply.to(request, ClientStatus.OK, null, result));
            } else {
                Throwable cause = error.getCause() != null ? error.getCause() : error;
                NodeId hint = cause instanceof NotLeaderException nle ? nle.leaderHint().orElse(null) : null;
                reply(envelope, ClientReply.to(request, ClientStatus.NOT_LEADER, hint, Bytes.EMPTY));
            }
        });
        return true;
    }

    /** The broken read path: answer a GET from whatever this replica happens to have applied. */
    private boolean answerLocally(Envelope envelope, ClientRequest request) {
        KVCommand command;
        try {
            command = KVCodec.decodeCommand(request.command());
        } catch (CodecException e) {
            return false;
        }
        if (!(command instanceof KVCommand.Get get)) {
            return false;
        }
        Bytes result = KVCodec.encode(new KVResult.GetResult(localStore.peek(get.key())));
        reply(envelope, ClientReply.to(request, ClientStatus.OK, null, result));
        return true;
    }

    /** Client requests handled so far (metrics). */
    public long requestsHandled() {
        return requests;
    }

    private void reply(Envelope request, ClientReply reply) {
        transport.send(request.from(), group, reply);
    }
}
