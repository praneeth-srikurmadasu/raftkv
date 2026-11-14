package dev.raftkv.transport;

import java.util.Objects;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.Message;

/**
 * A message plus its addressing: who sent it, to whom, for which Raft group.
 *
 * @param from    sending node
 * @param to      receiving node
 * @param group   Raft group the message belongs to
 * @param message the message itself
 */
public record Envelope(NodeId from, NodeId to, GroupId group, Message message) {

    public Envelope {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(message, "message");
    }
}
