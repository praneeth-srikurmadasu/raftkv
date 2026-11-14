package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.Objects;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;

/**
 * InstallSnapshot RPC (Figure 13): sends one chunk of a snapshot to a follower that is too far
 * behind to be caught up from the log.
 *
 * @param term              leader's term
 * @param leaderId          so followers can redirect clients
 * @param lastIncludedIndex the snapshot replaces all entries up through and including this index
 * @param lastIncludedTerm  term of lastIncludedIndex
 * @param offset            byte offset where this chunk is positioned in the snapshot file
 * @param data              raw bytes of the snapshot chunk
 * @param done              true if this is the last chunk
 */
public record InstallSnapshot(long term, NodeId leaderId, long lastIncludedIndex, long lastIncludedTerm,
                              long offset, Bytes data, boolean done) implements RaftMessage {

    public InstallSnapshot {
        nonNegative(term, "term");
        Objects.requireNonNull(leaderId, "leaderId");
        nonNegative(lastIncludedIndex, "lastIncludedIndex");
        nonNegative(lastIncludedTerm, "lastIncludedTerm");
        nonNegative(offset, "offset");
        Objects.requireNonNull(data, "data");
    }
}
