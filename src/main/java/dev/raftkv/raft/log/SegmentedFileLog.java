package dev.raftkv.raft.log;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

import dev.raftkv.common.Bytes;
import dev.raftkv.io.Disk;
import dev.raftkv.io.DiskFile;
import dev.raftkv.raft.storage.CorruptStateException;

/**
 * The Raft log on disk: a sequence of segment files (SPEC §3.7).
 *
 * <pre>
 *   log/log-00000000000000000001.seg   entries 1 .. 812
 *   log/log-00000000000000000813.seg   entries 813 .. (current)
 * </pre>
 *
 * Each entry is one record:
 * <pre>
 *   [length:4][crc32c:4][index:8][term:8][type:1][data:N]      length = 17 + N, CRC over index..data
 * </pre>
 *
 * <h2>Durability rules</h2>
 * <ul>
 *   <li>{@link #append} only writes; {@link #sync} forces every changed segment and, if segment
 *       files were created, the directory. Raft calls {@code sync} before acknowledging. Many
 *       appends can share one sync (<b>group commit</b>).</li>
 *   <li>Before writing to a new segment, the previous one is forced and the directory synced
 *       (so its name is durable too). So after a crash, damage can only be in the <b>last</b>
 *       segment.</li>
 *   <li>{@link #truncateSuffix} makes the truncation durable immediately: later segments are
 *       deleted newest-first with a directory sync after each, then the file is truncated and
 *       forced. New entries are written only afterwards, so a crash can never mix old and new
 *       entries or leave a gap between segments.</li>
 * </ul>
 *
 * <h2>Recovery ({@link #open})</h2>
 * Every record is read back and checked: length, CRC, consecutive index, non-decreasing term.
 * The first bad record in the last segment is a <b>torn write</b> from a crash: the file is
 * truncated there (those entries were never acknowledged, because they were never synced). A bad
 * record in any earlier segment, or a gap between segments, means real corruption: the node
 * refuses to start ({@link CorruptStateException}).
 *
 * <p>An in-memory index keeps (offset, term) for every entry, so {@link #termAt}, which Raft
 * calls constantly, never touches the disk.
 *
 * <h2>Compaction (Milestone 8)</h2>
 * {@link #compactPrefix} deletes whole segments that a snapshot covers, oldest first with a
 * directory sync after each delete (so a crash leaves an unbroken run of segments). Entries of a
 * partly covered segment stay in the file but are no longer visible. {@link #reset} deletes every
 * segment and starts a new, empty one named after the next index, so the log's position survives
 * a restart. After recovery the first segment's first index tells where the log starts; the term
 * just before it is learned from the snapshot ({@link #compactPrefix} with that index).
 */
public final class SegmentedFileLog implements RaftLog {

    static final int HEADER = 4 + 4;
    static final int FIXED = 8 + 8 + 1;
    /** Largest record we accept when reading (protects against a corrupt length field). */
    static final int MAX_RECORD = 64 * 1024 * 1024;

    private final Disk disk;
    private final Path dir;
    private final long segmentBytes;
    private final List<Segment> segments = new ArrayList<>();
    private boolean directoryDirty;
    private long lastIndex;
    private long lastTerm;
    /** Entries up to this index are covered by a snapshot and hidden (some may still be in the first file). */
    private long snapshotIndex;
    private long snapshotTerm;

    private static final class Segment {
        final long firstIndex;
        final Path path;
        final DiskFile file;
        long size;
        final LongArray offsets = new LongArray();
        final LongArray terms = new LongArray();
        boolean dirty;

        /** Opens (creating if needed) the segment file; the segment owns it until {@code file.close()}. */
        Segment(long firstIndex, Path path, Disk disk) {
            this.firstIndex = firstIndex;
            this.path = path;
            this.file = disk.open(path);
        }
    }

    private SegmentedFileLog(Disk disk, Path dir, long segmentBytes) {
        this.disk = disk;
        this.dir = dir;
        this.segmentBytes = segmentBytes;
    }

    /**
     * Opens (and recovers) the log in {@code dir}, creating it if empty.
     *
     * @param segmentBytes roll to a new segment once the current one reaches this size
     */
    public static SegmentedFileLog open(Disk disk, Path dir, long segmentBytes) {
        Objects.requireNonNull(disk, "disk");
        if (segmentBytes < 64) {
            throw new IllegalArgumentException("segmentBytes too small: " + segmentBytes);
        }
        disk.createDirectories(dir);
        SegmentedFileLog log = new SegmentedFileLog(disk, dir, segmentBytes);
        log.recover();
        return log;
    }

    static String segmentName(long firstIndex) {
        return String.format("log-%020d.seg", firstIndex);
    }

    // =====================================================================================
    // Recovery
    // =====================================================================================

    private void recover() {
        List<Long> firsts = new ArrayList<>();
        for (String name : disk.list(dir)) {
            if (name.startsWith("log-") && name.endsWith(".seg")) {
                firsts.add(Long.parseLong(name.substring(4, name.length() - 4)));
            }
        }
        firsts.sort(null);
        for (int i = 0; i < firsts.size(); i++) {
            long first = firsts.get(i);
            boolean isLast = i == firsts.size() - 1;
            if (i == 0 && first > 1) {
                // Older segments were compacted away: the log starts after a snapshot. Its term is
                // unknown until the snapshot tells us (compactPrefix).
                lastIndex = first - 1;
                snapshotIndex = first - 1;
            }
            if (first != lastIndex + 1) {
                throw new CorruptStateException("log segment " + segmentName(first) + " should start at "
                        + (lastIndex + 1) + " (gap or overlap between segments)");
            }
            Path path = dir.resolve(segmentName(first));
            Segment seg = openSegment(first, path);
            segments.add(seg);
            long fileSize = seg.file.size();
            long offset = 0;
            while (offset < fileSize) {
                String problem = readRecordAt(seg, offset, fileSize);
                if (problem != null) {
                    if (!isLast) {
                        throw new CorruptStateException("corrupt record in non-final segment " + seg.path
                                + " at offset " + offset + ": " + problem);
                    }
                    // Torn tail from a crash: drop it, durably, before anything else happens.
                    seg.file.truncate(offset);
                    seg.file.force();
                    break;
                }
                offset = seg.size;
            }
            seg.size = offset;
        }
        if (segments.isEmpty()) {
            createSegment(1);
        }
    }

    @Override
    public long snapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public long snapshotTerm() {
        return snapshotTerm;
    }

    /** Validates and indexes the record at {@code offset}; returns a description of the problem, or null. */
    private String readRecordAt(Segment seg, long offset, long fileSize) {
        if (fileSize - offset < HEADER + FIXED) {
            return "incomplete header";
        }
        ByteBuffer header = ByteBuffer.wrap(seg.file.readFully(offset, HEADER));
        int length = header.getInt();
        int crc = header.getInt();
        if (length < FIXED || length > MAX_RECORD) {
            return "bad length " + length;
        }
        if (fileSize - offset - HEADER < length) {
            return "incomplete record";
        }
        byte[] body = seg.file.readFully(offset + HEADER, length);
        CRC32C c = new CRC32C();
        c.update(body);
        if ((int) c.getValue() != crc) {
            return "CRC mismatch";
        }
        ByteBuffer b = ByteBuffer.wrap(body);
        long index = b.getLong();
        long term = b.getLong();
        byte type = b.get();
        if (index != lastIndex + 1) {
            return "index " + index + " where " + (lastIndex + 1) + " was expected";
        }
        if (term < lastTerm || term < 1) {
            return "term " + term + " after term " + lastTerm;
        }
        try {
            EntryType.fromCode(type);
        } catch (RuntimeException e) {
            return "unknown entry type " + type;
        }
        seg.offsets.add(offset);
        seg.terms.add(term);
        seg.size = offset + HEADER + length;
        lastIndex = index;
        lastTerm = term;
        return null;
    }

    // =====================================================================================
    // RaftLog
    // =====================================================================================

    @Override
    public long lastIndex() {
        return lastIndex;
    }

    @Override
    public long lastTerm() {
        return lastTerm;
    }

    @Override
    public long termAt(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        checkVisible(index);
        Segment s = segmentFor(index);
        return s.terms.get((int) (index - s.firstIndex));
    }

    @Override
    public LogEntry entryAt(long index) {
        checkVisible(index);
        Segment s = segmentFor(index);
        long offset = s.offsets.get((int) (index - s.firstIndex));
        int length = ByteBuffer.wrap(s.file.readFully(offset, 4)).getInt();
        ByteBuffer b = ByteBuffer.wrap(s.file.readFully(offset + HEADER, length));
        long idx = b.getLong();
        long term = b.getLong();
        EntryType type = EntryType.fromCode(b.get());
        byte[] data = new byte[length - FIXED];
        b.get(data);
        return new LogEntry(idx, term, type, Bytes.copyOf(data));
    }

    @Override
    public List<LogEntry> entriesFrom(long fromIndex, int maxCount) {
        if (fromIndex <= snapshotIndex || fromIndex > lastIndex + 1) {
            throw new IndexOutOfBoundsException("fromIndex " + fromIndex + " outside [" + (snapshotIndex + 1) + ", "
                    + (lastIndex + 1) + "]");
        }
        if (maxCount < 0) {
            throw new IllegalArgumentException("maxCount must be >= 0");
        }
        List<LogEntry> out = new ArrayList<>();
        for (long i = fromIndex; i <= lastIndex && out.size() < maxCount; i++) {
            out.add(entryAt(i));
        }
        return out;
    }

    @Override
    public void append(List<LogEntry> entries) {
        long expected = lastIndex + 1;
        long prevTerm = lastTerm;
        for (LogEntry e : entries) {
            if (e.index() != expected) {
                throw new IllegalArgumentException("append of index " + e.index() + ", expected " + expected);
            }
            if (e.term() < prevTerm) {
                throw new IllegalArgumentException("term would decrease from " + prevTerm + " to " + e.term()
                        + " at index " + e.index());
            }
            expected++;
            prevTerm = e.term();
        }
        for (LogEntry e : entries) {
            Segment seg = current();
            if (seg.size >= segmentBytes && seg.offsets.size() > 0) {
                // Roll over. First make the full segment completely durable: its contents AND its
                // file name (it was created at the previous roll, and names only become durable
                // with a directory sync). Then a crash can only ever damage the LAST segment;
                // otherwise a crash could keep a later segment's name but lose an earlier one's,
                // leaving a gap. (Found by the crash-at-any-moment property test.)
                if (seg.dirty) {
                    seg.file.force();
                    seg.dirty = false;
                }
                if (directoryDirty) {
                    disk.syncDirectory(dir);
                    directoryDirty = false;
                }
                seg = createSegment(e.index());
            }
            ByteBuffer record = encode(e);
            int length = record.remaining();
            seg.file.write(record, seg.size);
            seg.offsets.add(seg.size);
            seg.terms.add(e.term());
            seg.size += length;
            seg.dirty = true;
            lastIndex = e.index();
            lastTerm = e.term();
        }
    }

    @Override
    public void sync() {
        for (Segment seg : segments) {
            if (seg.dirty) {
                seg.file.force();
                seg.dirty = false;
            }
        }
        if (directoryDirty) {
            disk.syncDirectory(dir); // new segment files must survive a crash too
            directoryDirty = false;
        }
    }

    @Override
    public void truncateSuffix(long fromIndex) {
        if (fromIndex <= snapshotIndex || fromIndex > lastIndex + 1) {
            throw new IndexOutOfBoundsException("fromIndex " + fromIndex + " outside [" + (snapshotIndex + 1) + ", "
                    + (lastIndex + 1) + "]");
        }
        if (fromIndex == lastIndex + 1) {
            return;
        }
        int keep = segments.size() - 1;
        while (segments.get(keep).firstIndex > fromIndex) {
            keep--;
        }
        // 1. Delete every later segment, newest first, syncing the directory after EACH delete.
        //    File systems may persist unsynced deletions in any order; deleting one at a time
        //    guarantees that whatever survives a crash is still an unbroken run of segments.
        //    (One batch delete + one sync let a crash keep segments 14 and 32 but lose 20 and 26;
        //    found by the crash-at-any-moment property test.)
        if (directoryDirty) {
            disk.syncDirectory(dir);
            directoryDirty = false;
        }
        while (segments.size() - 1 > keep) {
            Segment gone = segments.remove(segments.size() - 1);
            gone.file.close();
            disk.delete(gone.path);
            disk.syncDirectory(dir);
        }
        // 2. Truncate the surviving segment and force it, before any new entry can be written.
        Segment seg = segments.get(keep);
        int keepEntries = (int) (fromIndex - seg.firstIndex);
        long newSize = keepEntries == seg.offsets.size() ? seg.size : seg.offsets.get(keepEntries);
        seg.file.truncate(newSize);
        seg.file.force();
        seg.dirty = false;
        seg.size = newSize;
        seg.offsets.truncate(keepEntries);
        seg.terms.truncate(keepEntries);
        lastIndex = fromIndex - 1;
        lastTerm = termAt(lastIndex);
    }

    @Override
    public void compactPrefix(long upToIndex, long upToTerm) {
        if (upToIndex < snapshotIndex || upToIndex > lastIndex) {
            throw new IndexOutOfBoundsException("cannot compact to " + upToIndex + ": log holds [" + snapshotIndex + ", "
                    + lastIndex + "]");
        }
        if (upToIndex == snapshotIndex) {
            // Typically right after recovery: we know where the log starts but not the term there.
            if (snapshotTerm != 0 && snapshotTerm != upToTerm) {
                throw new IllegalArgumentException("term at " + upToIndex + " is " + snapshotTerm + ", not " + upToTerm);
            }
            snapshotTerm = upToTerm;
            if (lastIndex == upToIndex) {
                lastTerm = upToTerm;
            }
            return;
        }
        long termThere = termAt(upToIndex);
        if (termThere != upToTerm) {
            throw new IllegalArgumentException("term at " + upToIndex + " is " + termThere + ", not " + upToTerm);
        }
        // Delete segments whose every entry is covered, oldest first, one durable delete at a time,
        // so whatever survives a crash is still an unbroken run. The last segment always stays:
        // it is where the next entry goes, and its name records where the log continues.
        if (directoryDirty) {
            disk.syncDirectory(dir);
            directoryDirty = false;
        }
        while (segments.size() > 1 && segments.get(1).firstIndex <= upToIndex + 1) {
            Segment gone = segments.remove(0);
            gone.file.close();
            disk.delete(gone.path);
            disk.syncDirectory(dir);
        }
        snapshotIndex = upToIndex;
        snapshotTerm = upToTerm;
    }

    @Override
    public void reset(long index, long term) {
        if (index < 0 || term < 0 || (index == 0) != (term == 0)) {
            throw new IllegalArgumentException("bad snapshot position " + index + "@t" + term);
        }
        if (directoryDirty) {
            disk.syncDirectory(dir);
            directoryDirty = false;
        }
        while (!segments.isEmpty()) {
            Segment gone = segments.remove(segments.size() - 1);
            gone.file.close();
            disk.delete(gone.path);
            disk.syncDirectory(dir);
        }
        snapshotIndex = index;
        snapshotTerm = term;
        lastIndex = index;
        lastTerm = term;
        createSegment(index + 1);
        disk.syncDirectory(dir); // the empty segment's name records where the log continues
        directoryDirty = false;
    }

    @Override
    public void close() {
        for (Segment seg : segments) {
            seg.file.close();
        }
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    /** Number of segment files (tests check rolling). */
    public int segmentCount() {
        return segments.size();
    }

    private Segment current() {
        return segments.get(segments.size() - 1);
    }

    private Segment createSegment(long firstIndex) {
        Path path = dir.resolve(segmentName(firstIndex));
        Segment seg = openSegment(firstIndex, path);
        segments.add(seg);
        directoryDirty = true;
        return seg;
    }

    /** The returned segment owns the open file; {@link #close()} (or truncation) closes it. */
    private Segment openSegment(long firstIndex, Path path) {
        return new Segment(firstIndex, path, disk);
    }

    private void checkVisible(long index) {
        if (index <= snapshotIndex || index > lastIndex) {
            throw new IndexOutOfBoundsException("index " + index + " outside log [" + (snapshotIndex + 1) + ", " + lastIndex + "]");
        }
    }

    private Segment segmentFor(long index) {
        int lo = 0;
        int hi = segments.size() - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (segments.get(mid).firstIndex <= index) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return segments.get(lo);
    }

    static ByteBuffer encode(LogEntry e) {
        int length = FIXED + e.data().length();
        ByteBuffer b = ByteBuffer.allocate(HEADER + length);
        b.putInt(length);
        b.putInt(0); // CRC placeholder
        b.putLong(e.index()).putLong(e.term()).put(e.type().code());
        e.data().writeTo(b);
        CRC32C c = new CRC32C();
        c.update(b.array(), HEADER, length);
        b.putInt(4, (int) c.getValue());
        b.flip();
        return b;
    }
}
