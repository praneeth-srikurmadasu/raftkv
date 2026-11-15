package dev.raftkv.raft;

/**
 * How a linearizable read is served (SPEC §4.4, thesis §6.4).
 */
public enum ReadMode {
    /** The read goes through the log like a write: simple, slow, always correct. */
    LOG,
    /**
     * ReadIndex: the leader notes its commit index, confirms with a heartbeat round acknowledged
     * by a majority that it is still the leader, waits until it has applied up to that index, and
     * reads locally. No log entry, one round trip.
     */
    READ_INDEX,
    /**
     * Lease: after a heartbeat round started at time t is acknowledged by a majority, the leader
     * may serve reads locally until {@code t + electionTimeoutMin / (1 + maxClockDrift)} without any
     * round trip. Safe only because followers refuse votes while they hear from a leader (stickiness)
     * and only if clocks drift less than {@code maxClockDrift}. Falls back to READ_INDEX when the
     * lease has expired.
     */
    LEASE
}
