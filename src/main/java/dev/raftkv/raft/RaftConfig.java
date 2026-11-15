package dev.raftkv.raft;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Timing and behavior settings for one Raft node.
 *
 * <p>Raft's timing requirement (§5.6): {@code broadcastTime << electionTimeout << MTBF}.
 * Heartbeats must arrive well within the election timeout, or followers would start
 * elections against a perfectly healthy leader.
 *
 * @param electionTimeoutMin  lower bound of the randomized election timeout (default 150 ms)
 * @param electionTimeoutMax  upper bound of the randomized election timeout (default 300 ms)
 * @param heartbeatInterval   how often a leader sends heartbeats (default 50 ms)
 * @param maxEntriesPerAppend most log entries in one AppendEntries message (default 64)
 * @param bugs                deliberately injected bugs, for the bug-detection tests only (default none)
 * @param snapshotThreshold   take a snapshot once this many applied entries are not yet covered by
 *                            one; 0 disables snapshots (default 10,000)
 * @param snapshotChunkBytes  size of one InstallSnapshot chunk (default 64 KiB)
 */
public record RaftConfig(Duration electionTimeoutMin, Duration electionTimeoutMax,
                         Duration heartbeatInterval, int maxEntriesPerAppend, Set<Bug> bugs,
                         long snapshotThreshold, int snapshotChunkBytes) {

    public static final long DEFAULT_SNAPSHOT_THRESHOLD = 10_000;
    public static final int DEFAULT_SNAPSHOT_CHUNK_BYTES = 64 * 1024;

    public RaftConfig {
        Objects.requireNonNull(electionTimeoutMin, "electionTimeoutMin");
        Objects.requireNonNull(electionTimeoutMax, "electionTimeoutMax");
        Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        if (electionTimeoutMin.isNegative() || electionTimeoutMin.isZero()) {
            throw new IllegalArgumentException("electionTimeoutMin must be > 0");
        }
        if (electionTimeoutMax.compareTo(electionTimeoutMin) < 0) {
            throw new IllegalArgumentException("electionTimeoutMax must be >= electionTimeoutMin");
        }
        if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()
                || heartbeatInterval.compareTo(electionTimeoutMin) >= 0) {
            throw new IllegalArgumentException("need 0 < heartbeatInterval < electionTimeoutMin");
        }
        if (maxEntriesPerAppend < 1) {
            throw new IllegalArgumentException("maxEntriesPerAppend must be >= 1");
        }
        if (snapshotThreshold < 0) {
            throw new IllegalArgumentException("snapshotThreshold must be >= 0 (0 = no snapshots)");
        }
        if (snapshotChunkBytes < 1) {
            throw new IllegalArgumentException("snapshotChunkBytes must be >= 1");
        }
        bugs = bugs.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(bugs));
    }

    /** Settings with the default snapshot behavior. */
    public RaftConfig(Duration electionTimeoutMin, Duration electionTimeoutMax, Duration heartbeatInterval,
                      int maxEntriesPerAppend, Set<Bug> bugs) {
        this(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend, bugs,
                DEFAULT_SNAPSHOT_THRESHOLD, DEFAULT_SNAPSHOT_CHUNK_BYTES);
    }

    /** 150-300 ms election timeout, 50 ms heartbeats, snapshots every 10,000 entries, no bugs. */
    public static RaftConfig defaults() {
        return new RaftConfig(Duration.ofMillis(150), Duration.ofMillis(300), Duration.ofMillis(50), 64, Set.of());
    }

    /** A copy of this config with the given bugs injected (tests only). */
    public RaftConfig withBugs(Bug first, Bug... rest) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend,
                EnumSet.of(first, rest), snapshotThreshold, snapshotChunkBytes);
    }

    /** A copy of this config with a different batch size. */
    public RaftConfig withMaxEntriesPerAppend(int max) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, max, bugs,
                snapshotThreshold, snapshotChunkBytes);
    }

    /** A copy of this config with different snapshot settings ({@code threshold} 0 = off). */
    public RaftConfig withSnapshots(long threshold, int chunkBytes) {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend, bugs,
                threshold, chunkBytes);
    }

    public boolean has(Bug bug) {
        return bugs.contains(bug);
    }
}
