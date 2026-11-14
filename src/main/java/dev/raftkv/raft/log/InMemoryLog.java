package dev.raftkv.raft.log;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link RaftLog} kept in an {@code ArrayList}. Fast and simple; used by unit tests and
 * simulations until the on-disk {@code SegmentedFileLog} arrives in Milestone 4.
 */
public final class InMemoryLog implements RaftLog {

    private final ArrayList<LogEntry> entries = new ArrayList<>();

    public InMemoryLog() {
    }

    /** A log that already holds {@code initial} (must start at index 1). */
    public InMemoryLog(List<LogEntry> initial) {
        append(initial);
    }

    @Override
    public long lastIndex() {
        return entries.size();
    }

    @Override
    public long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    @Override
    public long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        return entryAt(index).term();
    }

    @Override
    public LogEntry entryAt(long index) {
        if (index < 1 || index > lastIndex()) {
            throw new IndexOutOfBoundsException("index " + index + " outside log [1, " + lastIndex() + "]");
        }
        return entries.get((int) (index - 1));
    }

    @Override
    public List<LogEntry> entriesFrom(long fromIndex, int maxCount) {
        if (fromIndex < 1 || fromIndex > lastIndex() + 1) {
            throw new IndexOutOfBoundsException("fromIndex " + fromIndex + " outside [1, " + (lastIndex() + 1) + "]");
        }
        if (maxCount < 0) {
            throw new IllegalArgumentException("maxCount must be >= 0");
        }
        int from = (int) (fromIndex - 1);
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
        if (fromIndex < 1 || fromIndex > lastIndex() + 1) {
            throw new IndexOutOfBoundsException("fromIndex " + fromIndex + " outside [1, " + (lastIndex() + 1) + "]");
        }
        entries.subList((int) (fromIndex - 1), entries.size()).clear();
    }
}
