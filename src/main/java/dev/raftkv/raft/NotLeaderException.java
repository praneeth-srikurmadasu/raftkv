package dev.raftkv.raft;

import java.util.Optional;

import dev.raftkv.common.NodeId;

/**
 * The node can't (or can no longer) guarantee the proposal: it is not the leader, or it lost
 * leadership before the entry committed.
 *
 * <p>Important: when leadership is lost, the entry may <em>still</em> be committed later by the
 * next leader. A client that retries must therefore use the same request id so the state
 * machine can drop the duplicate (exactly-once sessions, Milestone 5).
 */
public final class NotLeaderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient NodeId leaderHint;

    public NotLeaderException(NodeId leaderHint) {
        super(leaderHint == null ? "not the leader (leader unknown)" : "not the leader; try " + leaderHint);
        this.leaderHint = leaderHint;
    }

    /** The node that is probably leader, if known. */
    public Optional<NodeId> leaderHint() {
        return Optional.ofNullable(leaderHint);
    }
}
