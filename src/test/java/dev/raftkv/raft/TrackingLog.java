package dev.raftkv.raft;

import java.util.List;

import dev.raftkv.raft.log.InMemoryLog;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.raft.log.RaftLog;

/**
 * Test wrapper around any {@link RaftLog} (an {@link InMemoryLog} or a {@code SegmentedFileLog}) that remembers the lowest index changed since the
 * invariant checker last looked. That lets the Log Matching check scan only what changed
 * instead of every log on every event.
 */
final class TrackingLog implements RaftLog {

    private final RaftLog delegate;
    private long dirtyFrom = 1; // everything is "new" until the first check

    TrackingLog(List<LogEntry> initial) {
        this(new InMemoryLog(initial));
    }

    TrackingLog(RaftLog delegate) {
        this.delegate = delegate;
    }

    RaftLog delegate() {
        return delegate;
    }

    /** Lowest index appended or truncated since the last call (Long.MAX_VALUE if none), then resets. */
    long takeDirtyFrom() {
        long d = dirtyFrom;
        dirtyFrom = Long.MAX_VALUE;
        return d;
    }

    long peekDirtyFrom() {
        return dirtyFrom;
    }

    @Override
    public long lastIndex() {
        return delegate.lastIndex();
    }

    @Override
    public long lastTerm() {
        return delegate.lastTerm();
    }

    @Override
    public long termAt(long index) {
        return delegate.termAt(index);
    }

    @Override
    public LogEntry entryAt(long index) {
        return delegate.entryAt(index);
    }

    @Override
    public List<LogEntry> entriesFrom(long fromIndex, int maxCount) {
        return delegate.entriesFrom(fromIndex, maxCount);
    }

    @Override
    public void append(List<LogEntry> entries) {
        if (!entries.isEmpty()) {
            dirtyFrom = Math.min(dirtyFrom, entries.get(0).index());
        }
        delegate.append(entries);
    }

    @Override
    public void sync() {
        delegate.sync();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void truncateSuffix(long fromIndex) {
        if (fromIndex <= delegate.lastIndex()) {
            dirtyFrom = Math.min(dirtyFrom, fromIndex);
        }
        delegate.truncateSuffix(fromIndex);
    }
}
