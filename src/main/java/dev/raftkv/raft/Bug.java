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
    LEADER_IGNORES_HIGHER_TERM,

    // ---- Milestone 3: replication

    /** Commit an entry as soon as a majority stores it, even from an earlier term (the Figure 8 bug, §5.4.2). */
    COMMIT_PREVIOUS_TERM_BY_COUNT,
    /** Follower sets commitIndex = min(leaderCommit, last log index) instead of the last <em>new</em> entry (Fig.2 rule 5). */
    COMMIT_INDEX_FROM_LAST_LOG_INDEX,
    /** Follower always truncates after prevLogIndex, even when entries match, so a stale message deletes newer entries. */
    TRUNCATE_ON_STALE_APPEND,
    /** A new leader does not append a no-op, so entries from earlier terms never commit without new traffic (§8). */
    NO_NOOP_ON_ELECTION,

    // ---- Milestone 4: persistence

    /** Acknowledge entries (and count the leader's own copy) without fsyncing the log first. */
    REPLY_BEFORE_FSYNC,

    // ---- Milestone 5: KV sessions

    /** The KV state machine ignores cached results, so a retried request executes again. */
    SESSION_DEDUP_DISABLED,

    // ---- Milestone 6: linearizability

    /** Any server answers GET straight from its own state machine, without the log or a leadership check. */
    READ_FROM_LOCAL_STATE,

    // ---- Milestone 10: shard migration

    /** A shard moves without its session table, so a retry that reaches the new owner executes again. */
    SESSIONS_NOT_MIGRATED,
    /** The old owner keeps serving a shard it has frozen for the new owner (BEPULLING). */
    SERVE_WHILE_BEPULLING,
    /** A group jumps straight to the latest configuration instead of applying them one by one. */
    SKIP_CONFIG_NUMBERS,
    /** The new owner tells the old one to delete a shard as soon as it fetched it, before installing it. */
    GC_BEFORE_INSERT_COMMITTED,
    /** Shard ownership is checked when a request arrives but not again when it is applied. */
    CHECK_OWNERSHIP_ON_RECEIVE_ONLY,

    // ---- Milestone 11: reads and elections

    /** ReadIndex serves the read without confirming leadership with a heartbeat round. */
    READINDEX_WITHOUT_HEARTBEAT,
    /** Followers grant votes even while they hear from a leader, which makes leases unsafe. */
    LEASE_WITHOUT_STICKINESS,
    /** A node restarts its election timer when it REJECTS a vote, so stale candidates can keep it from ever campaigning. */
    RESET_TIMER_ON_REJECTED_VOTE,

    // ---- Milestone 12: membership changes

    /** A new leader proposes a membership change before any entry of its own term has committed. */
    CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT
}
