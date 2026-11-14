package dev.raftkv.protocol;

import java.util.Objects;
import java.util.Optional;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;

/**
 * A server's answer to a {@link ClientRequest}.
 *
 * @param sequence   echoes the request's sequence number, so the client can match it
 * @param status     outcome
 * @param leaderHint for {@link ClientStatus#NOT_LEADER}: the node that is probably leader, or null
 * @param result     for {@link ClientStatus#OK}: the encoded operation result; empty otherwise
 */
public record ClientReply(long sequence, ClientStatus status, NodeId leaderHint, Bytes result) implements Message {

    public ClientReply {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(result, "result");
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence starts at 1, was " + sequence);
        }
    }

    public Optional<NodeId> leaderHintOptional() {
        return Optional.ofNullable(leaderHint);
    }
}
