package dev.raftkv.raft.storage;

import java.util.Objects;

import dev.raftkv.common.Bytes;

/**
 * A snapshot of a state machine (Raft §7, Figure 12): its full state after applying every entry
 * up to and including {@code lastIncludedIndex}.
 *
 * @param lastIncludedIndex the last log index the snapshot covers
 * @param lastIncludedTerm  the term of that entry (needed for the AppendEntries consistency check
 *                          of the first entry after the snapshot)
 * @param data              the state machine's serialized state
 */
public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, Bytes data) {

    public Snapshot {
        if (lastIncludedIndex < 1 || lastIncludedTerm < 1) {
            throw new IllegalArgumentException("snapshot must cover at least one entry: " + lastIncludedIndex
                    + "@t" + lastIncludedTerm);
        }
        Objects.requireNonNull(data, "data");
    }

    @Override
    public String toString() {
        return "Snapshot[" + lastIncludedIndex + "@t" + lastIncludedTerm + ", " + data.length() + " bytes]";
    }
}
