package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.List;
import java.util.Objects;

import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.LogEntry;

/**
 * AppendEntries RPC (Figure 2): replicates log entries; also used as heartbeat (no entries).
 *
 * @param term         leader's term
 * @param leaderId     so followers can redirect clients
 * @param prevLogIndex index of log entry immediately preceding the new ones
 * @param prevLogTerm  term of the prevLogIndex entry
 * @param entries      entries to store (empty for heartbeat); indexes are prevLogIndex+1, +2, ...
 * @param leaderCommit leader's commitIndex
 */
public record AppendEntries(long term, NodeId leaderId, long prevLogIndex, long prevLogTerm,
                            List<LogEntry> entries, long leaderCommit) implements RaftMessage {

    public AppendEntries {
        nonNegative(term, "term");
        Objects.requireNonNull(leaderId, "leaderId");
        nonNegative(prevLogIndex, "prevLogIndex");
        nonNegative(prevLogTerm, "prevLogTerm");
        nonNegative(leaderCommit, "leaderCommit");
        entries = List.copyOf(entries); // immutable, and rejects null elements
        for (int i = 0; i < entries.size(); i++) {
            long expected = prevLogIndex + 1 + i;
            if (entries.get(i).index() != expected) {
                throw new IllegalArgumentException("entry " + i + " has index " + entries.get(i).index()
                        + ", expected " + expected + " (entries must directly follow prevLogIndex)");
            }
        }
    }

    /** True if this is a heartbeat (carries no entries). */
    public boolean isHeartbeat() {
        return entries.isEmpty();
    }
}
