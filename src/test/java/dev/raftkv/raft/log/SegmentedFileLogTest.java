package dev.raftkv.raft.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import dev.raftkv.common.Bytes;
import dev.raftkv.io.sim.SimulatedDisk;
import dev.raftkv.io.sim.SimulatedDisk.CrashMode;
import dev.raftkv.raft.storage.CorruptStateException;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

/** On-disk behavior of {@link SegmentedFileLog}: rolling, recovery, torn tails, corruption, crashes. */
class SegmentedFileLogTest {

    private static final Path DIR = Path.of("/data/log");

    private static LogEntry entry(long index, long term) {
        return RaftLogContractTest.entry(index, term);
    }

    @Test
    void rollsToNewSegmentsAndNamesThemByFirstIndex() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, 256);
        log.append(RaftLogContractTest.entries(1, 40, 1));
        log.sync();
        assertThat(log.segmentCount()).isGreaterThan(3);
        assertThat(disk.list(DIR).get(0)).isEqualTo("log-00000000000000000001.seg");
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        SegmentedFileLog reopened = SegmentedFileLog.open(disk, DIR, 256);
        assertThat(reopened.segmentCount()).isEqualTo(log.segmentCount());
        assertThat(reopened.entriesFrom(1, 100)).isEqualTo(RaftLogContractTest.entries(1, 40, 1));
    }

    @Test
    void truncationAcrossSegmentsDeletesTheLaterFiles() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, 256);
        log.append(RaftLogContractTest.entries(1, 40, 1));
        log.sync();
        int before = disk.list(DIR).size();
        log.truncateSuffix(5);
        assertThat(disk.list(DIR).size()).isLessThan(before);
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED); // truncation must be durable on its own
        SegmentedFileLog reopened = SegmentedFileLog.open(disk, DIR, 256);
        assertThat(reopened.lastIndex()).isEqualTo(4L);
    }

    @Test
    void unsyncedEntriesMayBeLostButSyncedOnesNever() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, 1 << 20);
        log.append(RaftLogContractTest.entries(1, 10, 1));
        log.sync();
        log.append(RaftLogContractTest.entries(11, 20, 1)); // never synced
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(SegmentedFileLog.open(disk, DIR, 1 << 20).lastIndex()).isEqualTo(10L);
    }

    @Test
    void tornTailIsTruncatedAtTheFirstBadRecord() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, 1 << 20);
        log.append(RaftLogContractTest.entries(1, 5, 1));
        log.sync();
        Path seg = DIR.resolve(SegmentedFileLog.segmentName(1));
        long sizeAfter5;
        try (var f = disk.open(seg)) {
            sizeAfter5 = f.size();
        }
        log.append(RaftLogContractTest.entries(6, 6, 1));
        log.sync();
        disk.flipBit(seg, sizeAfter5 + 20, 1); // damage the 6th (last) record
        SegmentedFileLog reopened = SegmentedFileLog.open(disk, DIR, 1 << 20);
        assertThat(reopened.lastIndex()).isEqualTo(5L);
        reopened.append(List.of(entry(6, 2)));  // and the log is usable again
        reopened.sync();
        assertThat(SegmentedFileLog.open(disk, DIR, 1 << 20).termAt(6)).isEqualTo(2L);
    }

    @Test
    void corruptionInAnEarlierSegmentIsFatal() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, 256);
        log.append(RaftLogContractTest.entries(1, 40, 1));
        log.sync();
        disk.flipBit(DIR.resolve(SegmentedFileLog.segmentName(1)), 30, 2);
        assertThatThrownBy(() -> SegmentedFileLog.open(disk, DIR, 256))
                .isInstanceOf(CorruptStateException.class).hasMessageContaining("non-final segment");
    }

    @Test
    void aMissingSegmentIsFatal() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, 256);
        log.append(RaftLogContractTest.entries(1, 40, 1));
        log.sync();
        List<String> files = disk.list(DIR);
        disk.delete(DIR.resolve(files.get(1)));
        disk.syncDirectory(DIR);
        assertThatThrownBy(() -> SegmentedFileLog.open(disk, DIR, 256))
                .isInstanceOf(CorruptStateException.class).hasMessageContaining("gap");
    }

    /**
     * The big one. A random sequence of appends, syncs and truncations runs on a log; the power is
     * cut at a random disk operation, with a random crash mode. After recovery:
     * <ul>
     *   <li>the log passes its own recovery checks (CRCs, consecutive indexes, monotonic terms),</li>
     *   <li>it contains everything that was synced and not truncated afterwards, and</li>
     *   <li>every entry it holds is one that was really written at that index (no garbage, no mixing).</li>
     * </ul>
     */
    @Property(tries = 400)
    void recoversToAValidStateAfterACrashAtAnyMoment(@ForAll long seed) {
        Random r = new Random(seed);
        SimulatedDisk disk = new SimulatedDisk(new Random(seed));
        long segmentBytes = 128 + r.nextInt(600);
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, segmentBytes);

        List<LogEntry> model = new ArrayList<>();      // what the log holds right now
        List<LogEntry> durable = new ArrayList<>();    // what is guaranteed to survive
        List<List<LogEntry>> history = new ArrayList<>(); // every version ever written at each index
        long term = 1;
        disk.crashAtOperation(1 + r.nextInt(200));
        try {
            for (int step = 0; step < 60; step++) {
                int op = r.nextInt(10);
                if (op < 6) {
                    if (r.nextInt(5) == 0) {
                        term++;
                    }
                    List<LogEntry> batch = new ArrayList<>();
                    int n = 1 + r.nextInt(5);
                    for (int i = 0; i < n; i++) {
                        long index = model.size() + batch.size() + 1;
                        LogEntry e = new LogEntry(index, term, EntryType.COMMAND, Bytes.ofUtf8("v" + step + "-" + i));
                        batch.add(e);
                    }
                    // Record what we are ABOUT to write: a crash halfway through append may leave
                    // some of these records on disk, and recovery may legitimately return them.
                    for (LogEntry e : batch) {
                        while (history.size() < e.index()) {
                            history.add(new ArrayList<>());
                        }
                        history.get((int) e.index() - 1).add(e);
                    }
                    log.append(batch);
                    model.addAll(batch);
                } else if (op < 9) {
                    log.sync();
                    durable = new ArrayList<>(model);
                } else if (!model.isEmpty()) {
                    int from = 1 + r.nextInt(model.size());
                    // Once a truncation starts, the entries it removes are no longer guaranteed.
                    durable = new ArrayList<>(durable.subList(0, Math.min(durable.size(), from - 1)));
                    log.truncateSuffix(from);       // durable when it returns
                    model = new ArrayList<>(model.subList(0, from - 1));
                    term++;                          // like a new leader's entries
                }
            }
        } catch (SimulatedDisk.SimulatedCrash expected) {
            // the power went out
        }
        disk.crash(CrashMode.values()[r.nextInt(3)]);
        SegmentedFileLog recovered = SegmentedFileLog.open(disk, DIR, segmentBytes);

        assertThat(recovered.lastIndex()).as("seed %d: lost synced entries", seed).isGreaterThanOrEqualTo((long) durable.size());
        for (int i = 0; i < durable.size(); i++) {
            assertThat(recovered.entryAt(i + 1)).as("seed %d: synced entry %d changed", seed, i + 1).isEqualTo(durable.get(i));
        }
        for (long i = 1; i <= recovered.lastIndex(); i++) {
            LogEntry got = recovered.entryAt(i);
            assertThat(i <= history.size() && history.get((int) i - 1).contains(got)).as("seed %d: entry %d was never written: %s", seed, i, got).isTrue();
        }
    }

    @Test
    void compactionDeletesCoveredSegmentsOldestFirst() {
        SimulatedDisk disk = new SimulatedDisk(new Random(3));
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, 256);
        log.append(RaftLogContractTest.entries(1, 60, 1));
        log.sync();
        int before = log.segmentCount();
        log.compactPrefix(45, 1);
        assertThat(log.segmentCount()).isLessThan(before);
        List<String> files = disk.list(DIR).stream().filter(n -> n.endsWith(".seg")).sorted().toList();
        long firstStart = Long.parseLong(files.get(0).substring(4, files.get(0).length() - 4));
        assertThat(firstStart).isLessThanOrEqualTo(46L);
        assertThat(firstStart).isGreaterThan(1L);
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        SegmentedFileLog reopened = SegmentedFileLog.open(disk, DIR, 256);
        assertThat(reopened.snapshotIndex()).isEqualTo(firstStart - 1);
        assertThat(reopened.snapshotTerm()).isEqualTo(0L); // unknown until the snapshot tells it
        reopened.compactPrefix(45, 1);
        assertThat(reopened.entriesFrom(46, 100)).isEqualTo(RaftLogContractTest.entries(46, 60, 1));
    }

    /**
     * Like the test above, with snapshot compaction and full resets mixed in. As in RaftNode, the
     * snapshot is durable before {@code compactPrefix}/{@code reset} start. After the crash, the
     * recovered log must start no later than right after the snapshot, reconcile with it the way
     * {@code RaftNode.start} does, and still hold every synced entry after the snapshot.
     */
    @Property(tries = 400)
    void compactionAndResetSurviveACrashAtAnyMoment(@ForAll long seed) {
        Random r = new Random(seed);
        SimulatedDisk disk = new SimulatedDisk(new Random(seed));
        long segmentBytes = 128 + r.nextInt(600);
        SegmentedFileLog log = SegmentedFileLog.open(disk, DIR, segmentBytes);

        java.util.TreeMap<Long, LogEntry> model = new java.util.TreeMap<>();
        java.util.TreeMap<Long, LogEntry> durable = new java.util.TreeMap<>();
        java.util.Map<Long, List<LogEntry>> history = new java.util.HashMap<>();
        long snapIndex = 0;
        long snapTerm = 0;
        long last = 0;
        long term = 1;
        disk.crashAtOperation(1 + r.nextInt(300));
        try {
            for (int step = 0; step < 80; step++) {
                int op = r.nextInt(12);
                if (op < 6) {
                    if (r.nextInt(5) == 0) {
                        term++;
                    }
                    List<LogEntry> batch = new ArrayList<>();
                    int n = 1 + r.nextInt(5);
                    for (int i = 0; i < n; i++) {
                        LogEntry e = new LogEntry(last + 1 + i, term, EntryType.COMMAND, Bytes.ofUtf8("v" + step + "-" + i));
                        batch.add(e);
                        history.computeIfAbsent(e.index(), k -> new ArrayList<>()).add(e);
                    }
                    log.append(batch);
                    for (LogEntry e : batch) {
                        model.put(e.index(), e);
                    }
                    last += n;
                } else if (op < 8) {
                    log.sync();
                    durable = new java.util.TreeMap<>(model);
                } else if (op < 10 && last > snapIndex) {
                    long upTo = snapIndex + 1 + r.nextInt((int) (last - snapIndex));
                    long upToTerm = log.termAt(upTo);
                    snapIndex = upTo; // the snapshot is durable before compaction starts
                    snapTerm = upToTerm;
                    model.headMap(upTo, true).clear();
                    durable.headMap(upTo, true).clear();
                    log.compactPrefix(upTo, upToTerm);
                } else if (op == 10 && last > snapIndex) {
                    long from = snapIndex + 1 + r.nextInt((int) (last - snapIndex));
                    durable.tailMap(from, true).clear();
                    log.truncateSuffix(from);
                    model.tailMap(from, true).clear();
                    last = from - 1;
                    term++;
                } else if (op == 11 && r.nextInt(3) == 0) {
                    long index = Math.max(snapIndex + 1, last - 3 + r.nextInt(8));
                    term++;
                    snapIndex = index;
                    snapTerm = term;
                    model.clear();
                    durable.clear();
                    log.reset(index, term);
                    last = index;
                }
            }
        } catch (SimulatedDisk.SimulatedCrash expected) {
            // the power went out
        }
        disk.crash(CrashMode.values()[r.nextInt(3)]);
        SegmentedFileLog recovered = SegmentedFileLog.open(disk, DIR, segmentBytes);

        assertThat(recovered.snapshotIndex()).as("seed %d: log starts after the snapshot", seed).isLessThanOrEqualTo(snapIndex);
        if (snapIndex > 0) {
            if (recovered.contains(snapIndex)
                    && (snapIndex == recovered.snapshotIndex() || recovered.termAt(snapIndex) == snapTerm)) {
                recovered.compactPrefix(snapIndex, snapTerm);
            } else {
                recovered.reset(snapIndex, snapTerm);
            }
        }
        for (LogEntry e : durable.values()) {
            assertThat(recovered.contains(e.index()) && e.index() > recovered.snapshotIndex())
                    .as("seed %d: synced entry %d lost", seed, e.index()).isTrue();
            assertThat(recovered.entryAt(e.index())).as("seed %d: synced entry %d changed", seed, e.index()).isEqualTo(e);
        }
        for (long i = recovered.snapshotIndex() + 1; i <= recovered.lastIndex(); i++) {
            LogEntry got = recovered.entryAt(i);
            List<LogEntry> written = history.getOrDefault(i, List.of());
            assertThat(written.contains(got)).as("seed %d: entry %d was never written: %s", seed, i, got).isTrue();
        }
    }
}
