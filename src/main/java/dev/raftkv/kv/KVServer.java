package dev.raftkv.kv;

import java.util.Objects;

import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.raft.NotLeaderException;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.Role;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * The client-facing side of one replica (SPEC §4): turns {@link ClientRequest}s into Raft
 * proposals and Raft results into {@link ClientReply}s.
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
    private long requests;

    public KVServer(RaftNode raft, Transport transport, Clock clock) {
        this.raft = Objects.requireNonNull(raft, "raft");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.group = raft.group();
    }

    /** Handles one message from a client; ignores anything that is not a {@link ClientRequest}. */
    public void handle(Envelope envelope) {
        if (!(envelope.message() instanceof ClientRequest request)) {
            return;
        }
        requests++;
        if (raft.role() != Role.LEADER) {
            reply(envelope, new ClientReply(request.sequence(), ClientStatus.NOT_LEADER, raft.leaderId().orElse(null), Bytes.EMPTY));
            return;
        }
        try {
            KVCodec.decodeCommand(request.command()); // validate before it costs a log entry
        } catch (CodecException e) {
            reply(envelope, new ClientReply(request.sequence(), ClientStatus.INVALID_REQUEST, null, Bytes.EMPTY));
            return;
        }
        SessionCommand entry = new SessionCommand(request.clientId(), request.sequence(), request.ackedBelow(),
                clock.wallMillis(), request.command());
        raft.propose(KVCodec.encode(entry)).whenComplete((committed, error) -> {
            if (error == null) {
                KVCodec.Outcome outcome = KVCodec.decodeOutcome(committed.result());
                reply(envelope, new ClientReply(request.sequence(), outcome.status(), null, outcome.result()));
            } else {
                // The only way a proposal fails is losing (or never having) leadership.
                Throwable cause = error.getCause() != null ? error.getCause() : error;
                NodeId hint = cause instanceof NotLeaderException nle ? nle.leaderHint().orElse(null) : null;
                reply(envelope, new ClientReply(request.sequence(), ClientStatus.NOT_LEADER, hint, Bytes.EMPTY));
            }
        });
    }

    /** Client requests handled so far (metrics). */
    public long requestsHandled() {
        return requests;
    }

    private void reply(Envelope request, ClientReply reply) {
        transport.send(request.from(), group, reply);
    }
}
