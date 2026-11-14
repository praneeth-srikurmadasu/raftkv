package dev.raftkv.raft.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class InMemoryLogTest {

    private static List<LogEntry> entries(long... terms) {
        LogEntry[] out = new LogEntry[terms.length];
        for (int i = 0; i < terms.length; i++) {
            out[i] = LogEntry.noop(i + 1, terms[i]);
        }
        return List.of(out);
    }

    @Test
    void emptyLogHasVirtualEntryZero() {
        InMemoryLog log = new InMemoryLog();
        assertThat(log.lastIndex()).isEqualTo(0L);
        assertThat(log.lastTerm()).isEqualTo(0L);
        assertThat(log.termAt(0)).isEqualTo(0L);
        assertThat(log.contains(0)).isTrue();
        assertThat(log.contains(1)).isFalse();
    }

    @Test
    void appendAndRead() {
        InMemoryLog log = new InMemoryLog(entries(1, 1, 2));
        assertThat(log.lastIndex()).isEqualTo(3L);
        assertThat(log.lastTerm()).isEqualTo(2L);
        assertThat(log.termAt(2)).isEqualTo(1L);
        assertThat(log.entryAt(3)).isEqualTo(LogEntry.noop(3, 2));
        assertThat(log.entriesFrom(2, 10)).isEqualTo(List.of(LogEntry.noop(2, 1), LogEntry.noop(3, 2)));
        assertThat(log.entriesFrom(1, 1)).isEqualTo(List.of(LogEntry.noop(1, 1)));
        assertThat(log.entriesFrom(4, 10)).isEmpty();
    }

    @Test
    void rejectsGapsAndDecreasingTerms() {
        InMemoryLog log = new InMemoryLog(entries(1, 2));
        assertThatThrownBy(() -> log.append(List.of(LogEntry.noop(4, 2))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("expected 3");
        assertThatThrownBy(() -> log.append(List.of(LogEntry.noop(3, 1))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("decrease");
        assertThat(log.lastIndex()).isEqualTo(2L); // nothing was appended
    }

    @Test
    void truncateSuffixDeletesFromIndexOnward() {
        InMemoryLog log = new InMemoryLog(entries(1, 1, 2, 3));
        log.truncateSuffix(3);
        assertThat(log.lastIndex()).isEqualTo(2L);
        assertThat(log.lastTerm()).isEqualTo(1L);
        log.append(List.of(LogEntry.noop(3, 4)));   // a new leader's entry may replace the old ones
        assertThat(log.termAt(3)).isEqualTo(4L);
        log.truncateSuffix(4);                       // truncating past the end is a no-op
        assertThat(log.lastIndex()).isEqualTo(3L);
        assertThatThrownBy(() -> log.truncateSuffix(0)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void outOfRangeReadsFail() {
        InMemoryLog log = new InMemoryLog(entries(1));
        assertThatThrownBy(() -> log.entryAt(2)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.termAt(5)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> log.entriesFrom(3, 1)).isInstanceOf(IndexOutOfBoundsException.class);
    }
}
