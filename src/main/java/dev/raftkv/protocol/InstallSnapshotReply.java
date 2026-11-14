package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

/**
 * Reply to {@link InstallSnapshot} (Figure 13).
 *
 * @param term currentTerm, for leader to update itself
 */
public record InstallSnapshotReply(long term) implements RaftMessage {

    public InstallSnapshotReply {
        nonNegative(term, "term");
    }
}
