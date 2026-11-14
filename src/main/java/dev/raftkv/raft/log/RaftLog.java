package dev.raftkv.raft.log;

import java.util.List;

/**
 * The replicated log (Figure 2: {@code log[]}).
 *
 * <p>Indexes start at 1. Index 0 is a virtual "empty" position with term 0, which makes the
 * consistency check for the very first entry ({@code prevLogIndex = 0}) uniform.
 *
 * <p>Implementations check the two structural rules every Raft log obeys, so a bug in the
 * Raft core fails loudly instead of corrupting the log:
 * <ul>
 *   <li>indexes are contiguous (no gaps), and</li>
 *   <li>terms never decrease along the log.</li>
 * </ul>
 */
public interface RaftLog extends AutoCloseable {

    /** Index of the last entry, or 0 if the log is empty. */
    long lastIndex();

    /** Term of the last entry, or 0 if the log is empty. */
    long lastTerm();

    /** Term of the entry at {@code index}; 0 for index 0. Throws if the index is beyond the log. */
    long termAt(long index);

    /** True if the log has an entry at {@code index} (index 0 always "exists"). */
    default boolean contains(long index) {
        return index >= 0 && index <= lastIndex();
    }

    /** The entry at {@code index} (1..lastIndex). */
    LogEntry entryAt(long index);

    /** Entries from {@code fromIndex} (inclusive), at most {@code maxCount} of them. */
    List<LogEntry> entriesFrom(long fromIndex, int maxCount);

    /**
     * Appends entries that directly follow the current last entry. On a persistent log the
     * entries are not durable until {@link #sync()} returns.
     */
    void append(List<LogEntry> entries);

    /**
     * Makes every appended entry durable. Raft calls this before acknowledging entries to a
     * leader, and before a leader counts its own copy toward a majority. Default: no-op (memory).
     */
    default void sync() {
    }

    /**
     * Deletes the entry at {@code fromIndex} and every entry after it (Fig.2 AppendEntries rule 3).
     * On a persistent log the truncation is durable when this returns.
     */
    void truncateSuffix(long fromIndex);

    /** Releases files. Default: nothing to release. */
    @Override
    default void close() {
    }
}
