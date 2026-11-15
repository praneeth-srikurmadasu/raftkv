package dev.raftkv.raft;

import java.util.Random;

/**
 * Swarm testing for the optional machinery: each chaos seed decides on its own how nodes are
 * configured, so every combination meets every kind of fault.
 * <ul>
 *   <li><b>Snapshots</b> (Milestone 8): two thirds of the runs snapshot every 5-100 entries in
 *       chunks of 256 B - 8 KB, so compaction and InstallSnapshot happen constantly. The rest keep
 *       the default (effectively off for runs this short), so the plain log paths stay covered.</li>
 *   <li><b>Reads</b> (Milestone 11): LOG, READ_INDEX or LEASE, a third each.</li>
 *   <li><b>Pipelining</b>: 1 (stop-and-wait), 2 or 8 AppendEntries in flight per follower.</li>
 *   <li><b>Elections</b>: pre-vote and check-quorum each on in 3 of 4 runs (check-quorum always on
 *       with leases).</li>
 * </ul>
 * A separate random stream keeps the other per-seed choices of each chaos test unchanged.
 */
final class SnapshotSwarm {

    private SnapshotSwarm() {
    }

    static RaftConfig pick(long seed, RaftConfig config) {
        Random r = new Random(seed ^ 0x5A5A_1234_ABCDL);
        RaftConfig c = r.nextInt(3) == 0 ? config : config.withSnapshots(5 + r.nextInt(96), 256 + r.nextInt(7937));
        ReadMode mode = ReadMode.values()[r.nextInt(3)];
        int window = new int[] {1, 2, 8}[r.nextInt(3)];
        boolean preVote = r.nextInt(4) != 0;
        boolean checkQuorum = mode == ReadMode.LEASE || r.nextInt(4) != 0;
        return c.withReadMode(mode).withMaxInflightAppends(window).withElectionExtensions(preVote, checkQuorum);
    }
}
