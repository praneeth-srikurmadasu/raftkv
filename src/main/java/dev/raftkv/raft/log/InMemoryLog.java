package dev.raftkv.raft.log;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link RaftLog} kept in an {@code ArrayList}. Fast and simple; used by unit tests and
 * in-memory simulations. Entry {@code i} lives at position {@code i - snapshotIndex - 1}.
 */
public final class InMemoryLog implements RaftLog {

    private final ArrayList<LogEntry> entries = new ArrayList<>();
    private long snapshotIndex;
    private long snapshotTerm;

    public InMemoryLog() {
    }

    /** A log that already holds {@code initial} (must start at index 1). */
    public InMemoryLog(List<LogEntry> initial) {
        append(initial);
    }

    @Override
    public long lastIndex() {
        return snapshotIndex + entries.size();
    }

    @Override
    public long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.get(entries.size() - 1).term();
    }

    @Override
    public long snapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public long snapshotTerm() {
        return snapshotTerm;
    }

    @Override
    public long termAt(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        return entryAt(index).term();
    }

    @Override
    public LogEntry entryAt(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IndexOutOfBoundsException("index " + index + " outside log [" + (snapshotIndex + 1) + ", " + lastIndex() + "]");
        }
        return entries.get((int) (index - snapshotIndex - 1));
    }

    @Override
    public List<LogEntry> entriesFrom(long fromIndex, int maxCount) {
        if (fromIndex <= snapshotIndex || fromIndex > lastIndex() + 1) {
            throw new IndexOutOfBoundsException("fromIndex " + fromIndex + " outside [" + (snapshotIndex + 1) + ", "
                    + (lastIndex() + 1) + "]");
        }
        if (maxCount < 0) {
            throw new IllegalArgumentException("maxCount must be >= 0");
        }
        int from = (int) (fromIndex - snapshotIndex - 1);
        int to = (int) Math.min(entries.size(), (long) from + maxCount);
        return List.copyOf(entries.subList(from, to));
    }

    @Override
    public void append(List<LogEntry> newEntries) {
        long expectedIndex = lastIndex() + 1;
        long previousTerm = lastTerm();
        for (LogEntry e : newEntries) {
            if (e.index() != expectedIndex) {
                throw new IllegalArgumentException("append of index " + e.index() + ", expected " + expectedIndex);
            }
            if (e.term() < previousTerm) {
                throw new IllegalArgumentException("term would decrease from " + previousTerm + " to " + e.term()
                        + " at index " + e.index());
            }
            expectedIndex++;
            previousTerm = e.term();
        }
        entries.addAll(newEntries);
    }

    @Override
    public void truncateSuffix(long fromIndex) {
        if (fromIndex <= snapshotIndex || fromIndex > lastIndex() + 1) {
            throw new IndexOutOfBoundsException("fromIndex " + fromIndex + " outside [" + (snapshotIndex + 1) + ", "
                    + (lastIndex() + 1) + "]");
        }
        entries.subList((int) (fromIndex - snapshotIndex - 1), entries.size()).clear();
    }

    @Override
    public void compactPrefix(long upToIndex, long upToTerm) {
        if (!contains(upToIndex)) {
            throw new IndexOutOfBoundsException("cannot compact to " + upToIndex + ": log holds [" + snapshotIndex + ", "
                    + lastIndex() + "]");
        }
        if (termAt(upToIndex) != upToTerm) {
            throw new IllegalArgumentException("term at " + upToIndex + " is " + termAt(upToIndex) + ", not " + upToTerm);
        }
        entries.subList(0, (int) (upToIndex - snapshotIndex)).clear();
        snapshotIndex = upToIndex;
        snapshotTerm = upToTerm;
    }

    @Override
    public void reset(long index, long term) {
        if (index < 0 || term < 0 || (index == 0) != (term == 0)) {
            throw new IllegalArgumentException("bad snapshot position " + index + "@t" + term);
        }
        entries.clear();
        snapshotIndex = index;
        snapshotTerm = term;
    }
}
