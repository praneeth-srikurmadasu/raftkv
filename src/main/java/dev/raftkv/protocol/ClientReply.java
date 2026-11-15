package dev.raftkv.protocol;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;

/**
 * A server's answer to a {@link ClientRequest}.
 *
 * @param clientId   echoes the request's client: one process may host several clients (a shard
 *                   group's config poller talks to the controller, for example), and this is how
 *                   the reply finds the right one
 * @param sequence   echoes the request's sequence number, so the client can match it
 * @param status     outcome
 * @param leaderHint for {@link ClientStatus#NOT_LEADER}: the node that is probably leader, or null
 * @param result     for {@link ClientStatus#OK}: the encoded operation result; empty otherwise
 */
public record ClientReply(UUID clientId, long sequence, ClientStatus status, NodeId leaderHint, Bytes result)
        implements Message {

    public ClientReply {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(result, "result");
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence starts at 1, was " + sequence);
        }
    }

    /** The reply to {@code request}. */
    public static ClientReply to(ClientRequest request, ClientStatus status, NodeId leaderHint, Bytes result) {
        return new ClientReply(request.clientId(), request.sequence(), status, leaderHint, result);
    }

    public Optional<NodeId> leaderHintOptional() {
        return Optional.ofNullable(leaderHint);
    }
}
