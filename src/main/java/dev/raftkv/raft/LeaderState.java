package dev.raftkv.raft;

import java.util.Map;
import java.util.TreeMap;

import dev.raftkv.common.NodeId;

/**
 * Figure 2's "volatile state on leaders (reinitialized after election)".
 *
 * <ul>
 *   <li>{@code nextIndex[peer]}: index of the next log entry to send to that server
 *       (initialized to leader last log index + 1).</li>
 *   <li>{@code matchIndex[peer]}: index of highest log entry known to be replicated on that server
 *       (initialized to 0, increases monotonically).</li>
 * </ul>
 */
final class LeaderState {

    private final Map<NodeId, Long> nextIndex = new TreeMap<>();
    private final Map<NodeId, Long> matchIndex = new TreeMap<>();
    private final Map<NodeId, SnapshotTransfer> transfers = new TreeMap<>();

    /**
     * An InstallSnapshot transfer in progress to one follower. Up to a window of chunks is in
     * flight; each confirmation opens the window further. The follower accepts chunks only in
     * order, so after a lost chunk the leader goes back to the last confirmed offset (on the next
     * heartbeat) and resends from there ("go-back-N").
     */
    static final class SnapshotTransfer {
        final dev.raftkv.raft.storage.Snapshot snapshot;
        /** Bytes the follower has confirmed. */
        long acked;
        /** Where the next chunk to send starts ({@code >= acked}). */
        long sent;
        long lastSentNanos = Long.MIN_VALUE;

        SnapshotTransfer(dev.raftkv.raft.storage.Snapshot snapshot) {
            this.snapshot = snapshot;
        }
    }

    LeaderState(Iterable<NodeId> peers, long leaderLastIndex) {
        for (NodeId p : peers) {
            nextIndex.put(p, leaderLastIndex + 1);
            matchIndex.put(p, 0L);
        }
    }

    long nextIndex(NodeId peer) {
        return nextIndex.get(peer);
    }

    long matchIndex(NodeId peer) {
        return matchIndex.get(peer);
    }

    void setNextIndex(NodeId peer, long value) {
        if (value < 1) {
            throw new IllegalArgumentException("nextIndex must be >= 1, was " + value);
        }
        nextIndex.put(peer, value);
    }

    /** Raises matchIndex (never lowers it) and keeps nextIndex above it. */
    void recordMatch(NodeId peer, long index) {
        long match = Math.max(matchIndex.get(peer), index);
        matchIndex.put(peer, match);
        nextIndex.put(peer, Math.max(nextIndex.get(peer), match + 1));
    }

    SnapshotTransfer transfer(NodeId peer) {
        return transfers.get(peer);
    }

    SnapshotTransfer startTransfer(NodeId peer, dev.raftkv.raft.storage.Snapshot snapshot) {
        SnapshotTransfer t = new SnapshotTransfer(snapshot);
        transfers.put(peer, t);
        return t;
    }

    void endTransfer(NodeId peer) {
        transfers.remove(peer);
    }

    Iterable<Long> matchIndexes() {
        return matchIndex.values();
    }
}
