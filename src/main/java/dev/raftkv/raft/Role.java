package dev.raftkv.raft;

/**
 * The role a server plays (Raft §5.1, Figure 4, plus the thesis extensions of SPEC §3.3).
 *
 * <pre>
 *   FOLLOWER --(timeout)--> PRE_CANDIDATE --(pre-vote majority)--> CANDIDATE --(vote majority)--> LEADER
 *       ^                        |                                    |                            |
 *       +----(hears a leader, sees a higher term, or loses its quorum: check-quorum)---------------+
 *
 *   LEARNER: not a voter in the current membership; receives the log, never starts an election.
 * </pre>
 */
public enum Role {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER,
    LEARNER
}
