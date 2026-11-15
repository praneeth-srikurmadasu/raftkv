package dev.raftkv.raft.log;

import java.util.List;

/**
 * The replicated log (Figure 2: {@code log[]}).
 *
 * <p>Indexes start at 1. Index 0 is a virtual "empty" position with term 0, which makes the
 * consistency check for the very first entry ({@code prevLogIndex = 0}) uniform.
 *
 * <p><b>Compaction (Milestone 8, Raft §7).</b> Once a snapshot covers a prefix of the log, that
 * prefix is discarded. The log then holds entries {@code snapshotIndex()+1 .. lastIndex()}, and
 * the snapshot's last index plays the role index 0 played before: {@link #termAt} still answers
 * for {@code snapshotIndex()} (with the snapshot's term), so the AppendEntries consistency check
 * works unchanged for the first entry after the snapshot.
 *
 * <p>Implementations check the two structural rules every Raft log obeys, so a bug in the
 * Raft core fails loudly instead of corrupting the log:
 * <ul>
 *   <li>indexes are contiguous (no gaps), and</li>
 *   <li>terms never decrease along the log.</li>
 * </ul>
 */
public interface RaftLog {

    /** Index of the last entry, or 0 if the log is empty. */
    long lastIndex();

    /** Term of the last entry, or 0 if the log is empty. */
    long lastTerm();

    /**
     * Term of the entry at {@code index}, for {@code snapshotIndex() <= index <= lastIndex()}
     * (0 for index 0). Throws for indexes beyond the log or already compacted away.
     */
    long termAt(long index);

    /**
     * True if the log knows the term at {@code index}: {@code snapshotIndex() <= index <= lastIndex()}.
     * (The snapshot's own last index counts; index 0 always counts on a never-compacted log.)
     */
    default boolean contains(long index) {
        return index >= snapshotIndex() && index <= lastIndex();
    }

    /**
     * Last index covered by a snapshot (entries up to it have been discarded), or 0. Entries
     * start at {@code snapshotIndex() + 1}.
     */
    long snapshotIndex();

    /** Term at {@link #snapshotIndex()}; 0 if there is no snapshot (or after recovery, until set). */
    long snapshotTerm();

    /** The entry at {@code index} ({@code snapshotIndex()+1 .. lastIndex()}). */
    LogEntry entryAt(long index);

    /** Entries from {@code fromIndex} (inclusive, at least {@code snapshotIndex()+1}), at most {@code maxCount}. */
    List<LogEntry> entriesFrom(long fromIndex, int maxCount);

    /**
     * Discards every entry up to and including {@code upToIndex}, which a durable snapshot now
     * covers. {@code upToIndex} must be in the log ({@link #contains}); if the log knows its term,
     * it must equal {@code upToTerm} (passing the term lets a log that was recovered from disk
     * learn the term of its first position). Entries after it stay.
     */
    void compactPrefix(long upToIndex, long upToTerm);

    /**
     * Discards the whole log; it restarts empty right after {@code (index, term)}, which a durable
     * snapshot covers (Figure 13 rule 7: "discard the entire log"). Durable when this returns.
     */
    void reset(long index, long term);

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
     * {@code fromIndex} must be after {@link #snapshotIndex()} (snapshotted entries are committed
     * and can never be truncated). On a persistent log the truncation is durable when this returns.
     */
    void truncateSuffix(long fromIndex);

    /**
     * Releases files (persistent logs). Default: nothing to release.
     *
     * <p>Deliberately not {@link AutoCloseable}: most logs (in-memory ones, logs owned by a
     * long-lived node) are not scoped to a block, and IDEs would flag every one as a leak.
     */
    default void close() {
    }
}
