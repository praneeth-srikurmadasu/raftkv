package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

/**
 * Reply to {@link InstallSnapshot} (Figure 13, plus progress information for chunked transfer).
 *
 * <p>Figure 13's reply carries only the term. A real transfer also needs to know how far the
 * follower got, so it can be resumed after a lost chunk or a follower restart instead of
 * starting over:
 *
 * @param term              currentTerm, for leader to update itself
 * @param lastIncludedIndex which snapshot this reply is about
 * @param nextOffset        how many bytes of that snapshot the follower holds (where the next
 *                          chunk should start); meaningless when {@code installed}
 * @param installed         the follower now has a state at least as new as the snapshot (it
 *                          installed it, or it had already applied past it)
 */
public record InstallSnapshotReply(long term, long lastIncludedIndex, long nextOffset, boolean installed)
        implements RaftMessage {

    public InstallSnapshotReply {
        nonNegative(term, "term");
        nonNegative(lastIncludedIndex, "lastIncludedIndex");
        nonNegative(nextOffset, "nextOffset");
    }
}
