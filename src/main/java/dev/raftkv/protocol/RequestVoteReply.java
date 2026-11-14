package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

/**
 * Reply to {@link RequestVote} (Figure 2).
 *
 * @param term        voter's currentTerm, for the candidate to update itself
 * @param voteGranted true means the candidate received the vote
 * @param preVote     echoes the request, so a late pre-vote reply is never counted as a real vote
 */
public record RequestVoteReply(long term, boolean voteGranted, boolean preVote) implements Message {

    public RequestVoteReply {
        nonNegative(term, "term");
    }
}
