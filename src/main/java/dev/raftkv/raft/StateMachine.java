package dev.raftkv.raft;

import dev.raftkv.common.Bytes;

/**
 * The application that Raft replicates (the KV store, the shard controller, or a test recorder).
 *
 * <p>Contract, which every replica relies on to end up in the same state:
 * <ul>
 *   <li>{@link #apply} is called once per committed COMMAND entry, in log order, on the node's
 *       event loop.</li>
 *   <li>It must be <b>deterministic</b>: same commands in the same order give the same state and
 *       results on every replica. No clocks, no randomness, no {@code HashMap} iteration order.</li>
 *   <li>{@link #snapshot} serializes the complete state; {@link #restore} replaces the complete
 *       state with a serialized one. {@code restore(snapshot())} must give a state machine that
 *       behaves exactly like the original for every future command (Raft §7). Raft calls
 *       {@code restore} at startup (from the stored snapshot) and when a leader sends one.</li>
 * </ul>
 * State machines that never snapshot (simple test lambdas) can leave the defaults, as long as the
 * node's {@link RaftConfig#snapshotThreshold()} is 0.
 */
@FunctionalInterface
public interface StateMachine {

    /** Applies the command at {@code index} and returns the result for the waiting client. */
    Bytes apply(long index, Bytes command);

    /** The complete state, serialized. Called on the event loop right after an {@link #apply}. */
    default Bytes snapshot() {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support snapshots");
    }

    /** Replaces the complete state with {@code snapshot} (produced by {@link #snapshot()} on some replica). */
    default void restore(Bytes snapshot) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support snapshots");
    }
}
