package dev.raftkv.raft;

import java.util.Random;

/**
 * Swarm testing for snapshots: each chaos seed decides on its own whether nodes snapshot, how
 * often, and in what chunk size. Two thirds of the runs snapshot every 5-100 entries in chunks of
 * 256 B - 8 KB, so compaction and InstallSnapshot happen constantly under every kind of fault. (Tiny
 * chunks are covered by SnapshotTest; with chaos-sized snapshots of tens of KB they only make the
 * transfer slow, not more interesting: the follower accepts chunks strictly in order.) The
 * rest keep the configured default (effectively off for runs this short), so the plain log paths
 * stay covered too. A separate random stream keeps the other per-seed choices unchanged.
 */
final class SnapshotSwarm {

    private SnapshotSwarm() {
    }

    static RaftConfig pick(long seed, RaftConfig config) {
        Random r = new Random(seed ^ 0x5A5A_1234_ABCDL);
        if (r.nextInt(3) == 0) {
            return config;
        }
        return config.withSnapshots(5 + r.nextInt(96), 256 + r.nextInt(7937));
    }
}
