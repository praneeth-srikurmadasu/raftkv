package dev.raftkv.raft.storage;

/**
 * Durable storage for {@code currentTerm} and {@code votedFor}.
 *
 * <p>Contract: when {@link #save} returns, the state survives a crash. Raft relies on this:
 * a server must never reply to an RPC based on state it could forget (Figure 2, "updated on
 * stable storage before responding to RPCs"). Forgetting {@code votedFor} lets a server vote
 * twice in one term, which can elect two leaders in that term.
 *
 * <p>Milestone 2 uses {@link InMemoryMetaStore}; Milestone 4 adds the file-backed version
 * (write temp file, fsync, atomic rename, fsync directory).
 */
public interface MetaStore {

    /** The last saved state, or {@link PersistentState#INITIAL} on first boot. */
    PersistentState load();

    /** Durably replaces the stored state. */
    void save(PersistentState state);
}
