package dev.raftkv.raft.log;

import java.util.Objects;

import dev.raftkv.common.Bytes;

/**
 * One entry of the Raft log (Figure 2: "each entry contains command for state machine, and
 * term when entry was received by leader (first index is 1)").
 *
 * @param index position in the log, starting at 1
 * @param term  leader term when the entry was created, starting at 1
 * @param type  what the entry carries
 * @param data  the command / configuration bytes (empty for NOOP)
 */
public record LogEntry(long index, long term, EntryType type, Bytes data) {

    public LogEntry {
        if (index < 1) {
            throw new IllegalArgumentException("log index starts at 1, was " + index);
        }
        if (term < 1) {
            throw new IllegalArgumentException("entry term starts at 1, was " + term);
        }
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(data, "data");
    }

    /** Convenience for a no-op entry. */
    public static LogEntry noop(long index, long term) {
        return new LogEntry(index, term, EntryType.NOOP, Bytes.EMPTY);
    }
}
