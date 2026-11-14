package dev.raftkv.raft;

/**
 * Deliberate bugs that tests can switch on to prove the test suite would catch them (SPEC §9.6).
 *
 * <p>Each value disables one specific safety rule. Production code never enables any of them:
 * {@link RaftConfig#defaults()} has an empty set, and nothing outside the tests adds one. The
 * cost when disabled is one set lookup at the affected line.
 *
 * <p>If a test suite cannot detect a bug like this, the suite is too weak, no matter how many
 * tests pass. The bug-detection tests guard against that.
 */
public enum Bug {
    /** Grant votes without the "candidate's log is at least as up-to-date" check (§5.4.1). */
    VOTE_WITHOUT_LOG_CHECK,
    /** Ignore votedFor: grant a vote to every candidate in a term (§5.2: at most one vote per term). */
    VOTE_TWICE_IN_TERM,
    /** Persist the term but forget votedFor, so a restarted node can vote again in the same term. */
    VOTEDFOR_NOT_PERSISTED,
    /** A leader ignores messages with a higher term instead of stepping down (Fig.2 "All Servers"). */
    LEADER_IGNORES_HIGHER_TERM
}
