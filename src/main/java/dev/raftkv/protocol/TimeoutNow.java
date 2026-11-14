package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.Objects;

import dev.raftkv.common.NodeId;

/**
 * Leadership transfer (thesis §3.10): the leader tells a caught-up follower to start an
 * election immediately instead of waiting for its election timeout.
 *
 * @param term     leader's term
 * @param leaderId the leader handing over
 */
public record TimeoutNow(long term, NodeId leaderId) implements RaftMessage {

    public TimeoutNow {
        nonNegative(term, "term");
        Objects.requireNonNull(leaderId, "leaderId");
    }
}
