package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.Objects;

import dev.raftkv.common.NodeId;

/**
 * RequestVote RPC (Figure 2), plus two extensions from the Raft thesis.
 *
 * @param term               candidate's term (for a pre-vote: the term it <em>would</em> use)
 * @param candidateId        candidate requesting the vote
 * @param lastLogIndex       index of candidate's last log entry (§5.4)
 * @param lastLogTerm        term of candidate's last log entry (§5.4)
 * @param preVote            thesis §9.6: "would you vote for me?"; changes no state on the voter
 * @param leadershipTransfer thesis §3.10: sent after TimeoutNow; voters skip the leader-stickiness check
 */
public record RequestVote(long term, NodeId candidateId, long lastLogIndex, long lastLogTerm,
                          boolean preVote, boolean leadershipTransfer) implements RaftMessage {

    public RequestVote {
        nonNegative(term, "term");
        Objects.requireNonNull(candidateId, "candidateId");
        nonNegative(lastLogIndex, "lastLogIndex");
        nonNegative(lastLogTerm, "lastLogTerm");
    }
}
