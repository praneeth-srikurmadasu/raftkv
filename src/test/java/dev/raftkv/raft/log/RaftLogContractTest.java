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
}
