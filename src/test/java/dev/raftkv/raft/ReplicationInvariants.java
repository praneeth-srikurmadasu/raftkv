package dev.raftkv.raft;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;

/**
 * Figure 3's replication properties, checked after every simulated event (SPEC §9.3).
 * (State Machine Safety is checked inside {@link RecordingStateMachine} at the moment of apply.)
 *
 * <ul>
 *   <li><b>Log Matching:</b> if two logs contain an entry with the same index and term, the logs
 *       are identical in all entries up through that index. Checked by induction: every
 *       (index, term) pair ever seen must always have the same content and the same predecessor
 *       term. Only changed parts of logs are scanned.</li>
 *   <li><b>Leader Append-Only:</b> a leader never overwrites or deletes entries in its log.</li>
 *   <li><b>Committed entries never change:</b> once any node has an index committed with some
 *       term, no node ever commits a different term at that index.</li>
 *   <li><b>Leader Completeness:</b> if an entry is committed in a term, it is present in the
 *       logs of the leaders of all <em>later</em> terms. Checked when a node is first seen as
 *       leader. The term an entry was committed <em>in</em> is the term of the leader that first
 *       committed it (which may be later than the entry's own term). A leader of an earlier term
 *       can legitimately win its election after that commit, when its vote replies are delayed,
 *       and it need not have the entry.</li>
 *   <li>commitIndex never decreases within one run of a node.</li>
 * </ul>
 */
final class ReplicationInvariants implements Runnable {

    private record Key(long index, long term) {}

    private record Content(EntryType type, Bytes data, long prevTerm) {}

    private record Leadership(NodeId node, long term) {}

    private final SimulatedCluster cluster;
    private final Map<Key, Content> seenEntries = new HashMap<>();
    private final Map<Long, Long> committedTerm = new HashMap<>();
    /** index -> the term in which it was first committed (the committing leader's term). */
    private final Map<Long, Long> committedInTerm = new HashMap<>();
    private long maxCommitted;
    private final Map<Leadership, Long> leaderLastIndex = new HashMap<>();
    private final Map<RaftNode, Long> seenCommit = new IdentityHashMap<>();

    ReplicationInvariants(SimulatedCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void run() {
        for (RaftNode n : cluster.runningNodes()) {
            TrackingLog log = cluster.log(n.id());
            long dirty = log.takeDirtyFrom();

            // Leader Append-Only: while leading a term, nothing at or below the previous end changes.
            if (n.role() == Role.LEADER) {
                Leadership key = new Leadership(n.id(), n.currentTerm());
                Long previousLast = leaderLastIndex.get(key);
                if (previousLast == null) {
                    checkLeaderCompleteness(n, log);
                } else if (dirty <= previousLast) {
                    throw new AssertionError("Leader Append-Only violated: leader " + n.id() + " of t" + n.currentTerm()
                            + " changed its log at index " + dirty + " (had " + previousLast + " entries)");
                }
                leaderLastIndex.put(key, log.lastIndex());
            }

            // Log Matching, by induction over (index, term) -> (content, predecessor term).
            for (long i = Math.max(1, dirty); i <= log.lastIndex(); i++) {
                LogEntry e = log.entryAt(i);
                Content c = new Content(e.type(), e.data(), log.termAt(i - 1));
                Content earlier = seenEntries.putIfAbsent(new Key(i, e.term()), c);
                if (earlier != null && !earlier.equals(c)) {
                    throw new AssertionError("Log Matching violated at index " + i + " term " + e.term()
                            + " on " + n.id() + ": " + c + " vs earlier " + earlier);
                }
            }

            // Committed entries never change; commitIndex is monotonic per node run.
            long commit = n.commitIndex();
            long before = seenCommit.getOrDefault(n, 0L);
            if (commit < before) {
                throw new AssertionError(n.id() + " commitIndex went backwards: " + before + " -> " + commit);
            }
            for (long i = before + 1; i <= commit; i++) {
                long term = log.termAt(i);
                committedInTerm.putIfAbsent(i, n.currentTerm()); // the first node to commit i is its leader
                Long earlier = committedTerm.putIfAbsent(i, term);
                if (earlier != null && earlier != term) {
                    throw new AssertionError("committed entry changed at index " + i + ": t" + earlier
                            + " committed earlier, now " + n.id() + " commits t" + term);
                }
                maxCommitted = Math.max(maxCommitted, i);
            }
            seenCommit.put(n, commit);
        }
    }

    private void checkLeaderCompleteness(RaftNode leader, TrackingLog log) {
        for (long i = 1; i <= maxCommitted; i++) {
            Long term = committedTerm.get(i);
            if (term == null || committedInTerm.get(i) >= leader.currentTerm()) {
                continue; // Leader Completeness only binds leaders of later terms
            }
            if (i > log.lastIndex() || !Objects.equals(log.termAt(i), term)) {
                throw new AssertionError("Leader Completeness violated: new leader " + leader.id() + " of t"
                        + leader.currentTerm() + " lacks entry " + i + "@t" + term + ", committed in t" + committedInTerm.get(i));
            }
        }
    }

    long maxCommitted() {
        return maxCommitted;
    }
}
