package dev.raftkv.raft.storage;

import java.util.Objects;

/**
 * A {@link MetaStore} that lives in memory.
 *
 * <p>In simulations the test harness keeps this object when it "crashes" a node and hands it
 * to the restarted node, so it behaves exactly like a disk that never loses a completed
 * write. (Disks that <em>do</em> lose unsynced writes arrive with SimulatedDisk in Milestone 4.)
 */
public final class InMemoryMetaStore implements MetaStore {

    private PersistentState state;
    private long saves;

    public InMemoryMetaStore() {
        this(PersistentState.INITIAL);
    }

    public InMemoryMetaStore(PersistentState initial) {
        this.state = Objects.requireNonNull(initial, "initial");
    }

    @Override
    public PersistentState load() {
        return state;
    }

    @Override
    public void save(PersistentState newState) {
        this.state = Objects.requireNonNull(newState, "newState");
        saves++;
    }

    /** How many times {@link #save} was called (tests use it to check what was persisted when). */
    public long saveCount() {
        return saves;
    }
}
