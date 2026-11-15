package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

/**
 * Reply to {@link AppendEntries} (Figure 2), with the fast-backtracking fields of §5.3.
 *
 * @param term          follower's currentTerm, for the leader to update itself
 * @param success       true if follower contained an entry matching prevLogIndex and prevLogTerm
 * @param matchIndex    on success: the last index known to match the leader's log (lets the
 *                      leader handle pipelined / reordered replies safely)
 * @param conflictIndex on failure: where the leader should retry from (SPEC §3.5)
 * @param conflictTerm  on failure: term of the conflicting entry, or 0 if the follower's log is too short
 * @param round         the {@link AppendEntries#round()} this answers
 */
public record AppendEntriesReply(long term, boolean success, long matchIndex,
                                 long conflictIndex, long conflictTerm, long round) implements RaftMessage {

    public AppendEntriesReply {
        nonNegative(term, "term");
        nonNegative(matchIndex, "matchIndex");
        nonNegative(conflictIndex, "conflictIndex");
        nonNegative(conflictTerm, "conflictTerm");
        nonNegative(round, "round");
    }

    /** A reply outside any heartbeat round. */
    public AppendEntriesReply(long term, boolean success, long matchIndex, long conflictIndex, long conflictTerm) {
        this(term, success, matchIndex, conflictIndex, conflictTerm, 0);
    }
}
