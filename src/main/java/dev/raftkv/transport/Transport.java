package dev.raftkv.transport;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.Message;

/**
 * Sends messages from one node to its peers.
 *
 * <p>Delivery is <b>best effort</b>: a message may be lost, delayed, duplicated, or reordered.
 * Raft is designed for exactly this kind of network, so the interface promises nothing more.
 * Being an interface lets tests swap the real TCP transport (Milestone 7) for
 * {@code SimulatedNetwork}, which can inject all of those faults deterministically.
 */
public interface Transport {

    /** The node this transport sends from. */
    NodeId self();

    /** Sends {@code message} to {@code to} for {@code group}. Never blocks; never throws on network failure. */
    void send(NodeId to, GroupId group, Message message);
}
