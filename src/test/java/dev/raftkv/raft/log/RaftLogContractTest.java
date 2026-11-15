package dev.raftkv.raft.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import dev.raftkv.common.Bytes;
import org.junit.jupiter.api.Test;

/**
 * The behavior every {@link RaftLog} must have. Concrete test classes plug in an
 * implementation; the same tests then run against the in-memory and the on-disk log.
 */
abstract class RaftLogContractTest {

    /** A fresh, empty log. */
    abstract RaftLog newLog();

    /** Reopens the log from whatever is durable (persistent logs) or returns it unchanged (memory). */
    abstract RaftLog reopen(RaftLog log);

    static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, EntryType.COMMAND, Bytes.ofUtf8("cmd-" + index + "-" + term));
    }

    static List<LogEntry> entries(long from, long to, long term) {
        List<LogEntry> out = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            out.add(entry(i, term));
        }
        return out;
    }

    @Test
    void emptyLog() {
        RaftLog log = newLog();
        assertThat(log.lastIndex()).isEqualTo(0L);
        assertThat(log.lastTerm()).isEqualTo(0L);
        assertThat(log.termAt(0)).isEqualTo(0L);
        assertThat(log.entriesFrom(1, 10)).isEmpty();
    }

    @Test
    void appendSyncReopenRead() {
        RaftLog log = newLog();
        log.append(entries(1, 50, 1));
        log.append(entries(51, 100, 2));
        log.sync();
        log = reopen(log);
        assertThat(log.lastIndex()).isEqualTo(100L);
        assertThat(log.lastTerm()).isEqualTo(2L);
        assertThat(log.termAt(50)).isEqualTo(1L);
        assertThat(log.entryAt(77)).isEqualTo(entry(77, 2));
        assertThat(log.entriesFrom(98, 10)).isEqualTo(entries(98, 100, 2));
    }

    @Test
    void preservesEveryEntryTypeAndPayload() {
        RaftLog log = newLog();
        byte[] big = new byte[3000];
        new Random(1).nextBytes(big);
        List<LogEntry> in = List.of(
                LogEntry.noop(1, 1),
                new LogEntry(2, 1, EntryType.CONFIGURATION, Bytes.ofUtf8("cfg")),
                new LogEntry(3, 2, EntryType.COMMAND, Bytes.copyOf(big)),
                new LogEntry(4, 2, EntryType.COMMAND, Bytes.EMPTY));
        log.append(in);
        log.sync();
        log = reopen(log);
        assertThat(log.entriesFrom(1, 10)).isEqualTo(in);
    }

    @Test
    void truncateThenAppendANewTerm() {
        RaftLog log = newLog();
        log.append(entries(1, 80, 1));
        log.sync();
        log.truncateSuffix(31);
        assertThat(log.lastIndex()).isEqualTo(30L);
        log.append(entries(31, 40, 3));
        log.sync();
        log = reopen(log);
        assertThat(log.lastIndex()).isEqualTo(40L);
        assertThat(log.termAt(30)).isEqualTo(1L);
        assertThat(log.termAt(31)).isEqualTo(3L);
        assertThat(log.entryAt(35)).isEqualTo(entry(35, 3));
    }

    @Test
    void truncateEverything() {
        RaftLog log = newLog();
        log.append(entries(1, 20, 1));
        log.truncateSuffix(1);
        assertThat(log.lastIndex()).isEqualTo(0L);
        log.append(entries(1, 3, 2));
        log.sync();
        log = reopen(log);
        assertThat(log.entriesFrom(1, 10)).isEqualTo(entries(1, 3, 2));
    }

    @Test
    void rejectsStructuralViolations() {
        RaftLog log = newLog();
        log.append(entries(1, 3, 2));
        assertThatThrownBy(() -> log.append(List.of(entry(5, 2)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> log.append(List.of(entry(4, 1)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> log.entryAt(4)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.truncateSuffix(0)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThat(log.lastIndex()).isEqualTo(3L);
    }

    // ------------------------------------------------------------------ compaction (Milestone 8)

    @Test
    void compactPrefixHidesCoveredEntriesAndKeepsTheRest() {
        RaftLog log = newLog();
        log.append(entries(1, 50, 1));
        log.append(entries(51, 100, 2));
        log.sync();
        log.compactPrefix(60, 2);
        assertThat(log.snapshotIndex()).isEqualTo(60L);
        assertThat(log.snapshotTerm()).isEqualTo(2L);
        assertThat(log.termAt(60)).isEqualTo(2L);
        assertThat(log.contains(60)).isTrue();
        assertThat(log.contains(59)).isFalse();
        assertThat(log.lastIndex()).isEqualTo(100L);
        assertThat(log.entryAt(61)).isEqualTo(entry(61, 2));
        assertThat(log.entriesFrom(99, 10)).isEqualTo(entries(99, 100, 2));
        assertThatThrownBy(() -> log.entryAt(60)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.termAt(59)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.entriesFrom(60, 1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.truncateSuffix(60)).isInstanceOf(IndexOutOfBoundsException.class);

        // Truncation after the snapshot and appends still work.
        log.truncateSuffix(91);
        log.append(entries(91, 95, 3));
        log.sync();
        assertThat(log.lastIndex()).isEqualTo(95L);
        assertThat(log.lastTerm()).isEqualTo(3L);

        // After a restart a persistent log may show more (partly compacted files keep old records)
        // but never less, and the snapshot position reconciles it exactly as RaftNode.start does.
        List<LogEntry> expected = log.entriesFrom(61, 100);
        RaftLog reopened = reopen(log);
        assertThat(reopened.snapshotIndex()).isLessThanOrEqualTo(60L);
        reopened.compactPrefix(60, 2);
        assertThat(reopened.snapshotIndex()).isEqualTo(60L);
        assertThat(reopened.termAt(60)).isEqualTo(2L);
        assertThat(reopened.entriesFrom(61, 100)).isEqualTo(expected);
    }

    @Test
    void compactingEverythingLeavesAnEmptyLogThatContinuesAfterIt() {
        RaftLog log = newLog();
        log.append(entries(1, 30, 4));
        log.sync();
        log.compactPrefix(30, 4);
        assertThat(log.lastIndex()).isEqualTo(30L);
        assertThat(log.lastTerm()).isEqualTo(4L);
        assertThat(log.entriesFrom(31, 10)).isEmpty();
        log.append(entries(31, 33, 5));
        log.sync();
        RaftLog reopened = reopen(log);
        reopened.compactPrefix(30, 4);
        assertThat(reopened.entriesFrom(31, 10)).isEqualTo(entries(31, 33, 5));
    }

    @Test
    void resetDiscardsTheWholeLogAndContinuesAfterTheSnapshot() {
        RaftLog log = newLog();
        log.append(entries(1, 30, 1));
        log.sync();
        log.reset(100, 5);
        assertThat(log.snapshotIndex()).isEqualTo(100L);
        assertThat(log.lastIndex()).isEqualTo(100L);
        assertThat(log.lastTerm()).isEqualTo(5L);
        assertThat(log.termAt(100)).isEqualTo(5L);
        assertThat(log.contains(30)).isFalse();
        assertThatThrownBy(() -> log.append(entries(1, 1, 5))).isInstanceOf(IllegalArgumentException.class);
        log.append(entries(101, 110, 6));
        log.sync();
        RaftLog reopened = reopen(log);
        assertThat(reopened.snapshotIndex()).isEqualTo(100L);
        reopened.compactPrefix(100, 5);
        assertThat(reopened.lastIndex()).isEqualTo(110L);
        assertThat(reopened.termAt(100)).isEqualTo(5L);
        assertThat(reopened.entryAt(101)).isEqualTo(entry(101, 6));
    }

    @Test
    void compactionRejectsWrongTermsAndPositions() {
        RaftLog log = newLog();
        log.append(entries(1, 10, 2));
        assertThatThrownBy(() -> log.compactPrefix(5, 3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> log.compactPrefix(11, 2)).isInstanceOf(IndexOutOfBoundsException.class);
        log.compactPrefix(5, 2);
        assertThatThrownBy(() -> log.compactPrefix(4, 2)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.reset(-1, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> log.reset(5, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
