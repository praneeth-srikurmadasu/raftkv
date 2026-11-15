package dev.raftkv.raft.storage;

import java.util.Optional;

/**
 * Durable storage for the latest snapshot of one Raft group (SPEC §3.8).
 *
 * <p>Contract: when {@link #save} returns, the snapshot survives a crash, and {@link #load}
 * returns it (or a newer one). Raft discards log entries only after that, so at every moment the
 * snapshot plus the remaining log hold every committed entry.
 */
public interface SnapshotStore {

    /** The latest saved snapshot, if any. */
    Optional<Snapshot> load();

    /** Durably replaces the stored snapshot. */
    void save(Snapshot snapshot);
}
