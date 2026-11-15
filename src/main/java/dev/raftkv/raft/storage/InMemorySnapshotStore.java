package dev.raftkv.raft.storage;

import java.util.Objects;
import java.util.Optional;

/** A {@link SnapshotStore} in memory (tests and simulations; survives "crashes" of the node object). */
public final class InMemorySnapshotStore implements SnapshotStore {

    private Snapshot snapshot;
    private int saves;

    @Override
    public Optional<Snapshot> load() {
        return Optional.ofNullable(snapshot);
    }

    @Override
    public void save(Snapshot s) {
        snapshot = Objects.requireNonNull(s, "snapshot");
        saves++;
    }

    /** How many snapshots were saved (tests). */
    public int saves() {
        return saves;
    }
}
