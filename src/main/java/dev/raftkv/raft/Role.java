package dev.raftkv.raft;

/**
 * The role a server plays (Raft §5.1, Figure 4).
 *
 * <pre>
 *   FOLLOWER --(election timeout)--> CANDIDATE --(majority of votes)--> LEADER
 *       ^                               |                                 |
 *       +----(discovers current leader or a higher term)------------------+
 * </pre>
 *
 * PRE_CANDIDATE (pre-vote) and LEARNER (non-voting member) are added in later milestones.
 */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
