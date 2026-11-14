package dev.raftkv.raft.storage;

import java.util.Optional;

import dev.raftkv.common.NodeId;

/**
 * The two small pieces of Figure 2's "persistent state on all servers" that are not the log.
 *
 * @param currentTerm latest term server has seen (initialized to 0 on first boot, increases monotonically)
 * @param votedFor    candidate that received this server's vote in currentTerm, or {@code null} if none
 */
public record PersistentState(long currentTerm, NodeId votedFor) {

    /** State of a server that has never run before. */
    public static final PersistentState INITIAL = new PersistentState(0, null);

    public PersistentState {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm must be >= 0, was " + currentTerm);
        }
    }

    public Optional<NodeId> votedForOptional() {
        return Optional.ofNullable(votedFor);
    }
}
