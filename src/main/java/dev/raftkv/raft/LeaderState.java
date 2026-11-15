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
    /** Pipelining: highest index already sent (and not known to be lost) per follower. */
    private final Map<NodeId, Long> sentUpTo = new TreeMap<>();
    /** Pipelining: AppendEntries sent since the window was last reset, not yet answered with progress. */
    private final Map<NodeId, Integer> inflight = new TreeMap<>();
    /** Pipelining: whether we already went back after a gap since the last progress (at most once). */
    private final Map<NodeId, Boolean> wentBack = new TreeMap<>();
    /** Pipelining: when a reply from each follower last made progress (go-back-N after a silence). */
    private final Map<NodeId, Long> lastProgressNanos = new TreeMap<>();
    /** Check-quorum: when we last heard any reply of the current term from each follower. */
    private final Map<NodeId, Long> lastAckNanos = new TreeMap<>();
    /** ReadIndex / leases: the highest heartbeat round each follower has echoed. */
    private final Map<NodeId, Long> ackedRound = new TreeMap<>();

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

    LeaderState(Iterable<NodeId> peers, long leaderLastIndex, long nowNanos) {
        for (NodeId p : peers) {
            addPeer(p, leaderLastIndex, nowNanos);
        }
    }

    /** A server joined the membership (or the leader just started): begin replicating to it. */
    void addPeer(NodeId p, long leaderLastIndex, long nowNanos) {
        if (nextIndex.containsKey(p)) {
            return;
        }
        nextIndex.put(p, leaderLastIndex + 1);
        matchIndex.put(p, 0L);
        sentUpTo.put(p, leaderLastIndex);
        inflight.put(p, 0);
        lastAckNanos.put(p, nowNanos);
        lastProgressNanos.put(p, nowNanos);
        wentBack.put(p, false);
        ackedRound.put(p, 0L);
    }

    /** A server left the membership: forget it. */
    void removePeer(NodeId p) {
        nextIndex.remove(p);
        matchIndex.remove(p);
        transfers.remove(p);
        sentUpTo.remove(p);
        inflight.remove(p);
        lastAckNanos.remove(p);
        lastProgressNanos.remove(p);
        wentBack.remove(p);
        ackedRound.remove(p);
    }

    boolean hasPeer(NodeId p) {
        return nextIndex.containsKey(p);
    }

    java.util.Set<NodeId> peers() {
        return nextIndex.keySet();
    }

    long sentUpTo(NodeId peer) {
        return sentUpTo.get(peer);
    }

    int inflight(NodeId peer) {
        return inflight.get(peer);
    }

    /** One more batch in flight, covering entries up to {@code lastSent}. */
    void recordSend(NodeId peer, long lastSent) {
        sentUpTo.put(peer, Math.max(sentUpTo.get(peer), lastSent));
        inflight.put(peer, inflight.get(peer) + 1);
    }

    /** A reply made progress: one fewer batch in flight (none, if everything sent is now matched). */
    void recordProgress(NodeId peer, long nowNanos) {
        lastProgressNanos.put(peer, nowNanos);
        wentBack.put(peer, false);
        int n = matchIndex.get(peer) >= sentUpTo.get(peer) ? 0 : Math.max(0, inflight.get(peer) - 1);
        inflight.put(peer, n);
    }

    /**
     * A batch arrived before the one in front of it (the follower's log ends at
     * {@code followerLast}): resend from there. Allowed once per progress, so that a burst of
     * out-of-order rejections (or duplicated ones) can't multiply into bursts of resends.
     */
    boolean goBackOnce(NodeId peer, long followerLast) {
        if (wentBack.get(peer) || followerLast >= sentUpTo.get(peer) || followerLast < matchIndex.get(peer)) {
            return false;
        }
        wentBack.put(peer, true);
        sentUpTo.put(peer, followerLast);
        inflight.put(peer, 0);
        return true;
    }

    long lastProgressNanos(NodeId peer) {
        return lastProgressNanos.get(peer);
    }

    /** Go back to what is confirmed: resend from nextIndex with an empty window (heartbeats, rejections). */
    void resetWindow(NodeId peer) {
        sentUpTo.put(peer, nextIndex.get(peer) - 1);
        inflight.put(peer, 0);
    }

    void recordAck(NodeId peer, long round, long nowNanos) {
        lastAckNanos.put(peer, nowNanos);
        ackedRound.put(peer, Math.max(ackedRound.get(peer), round));
    }

    long lastAckNanos(NodeId peer) {
        return lastAckNanos.get(peer);
    }

    long ackedRound(NodeId peer) {
        return ackedRound.get(peer);
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
        sentUpTo.put(peer, Math.min(sentUpTo.get(peer), value - 1));
    }

    /** Raises matchIndex (never lowers it) and keeps nextIndex above it. */
    void recordMatch(NodeId peer, long index) {
        long match = Math.max(matchIndex.get(peer), index);
        matchIndex.put(peer, match);
        nextIndex.put(peer, Math.max(nextIndex.get(peer), match + 1));
        sentUpTo.put(peer, Math.max(sentUpTo.get(peer), match));
        if (match >= sentUpTo.get(peer)) {
            inflight.put(peer, 0); // everything we sent is confirmed (e.g. by an installed snapshot)
        }
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
