package dev.raftkv.raft;

import dev.raftkv.common.Bytes;

/**
 * The application that Raft replicates (the KV store from Milestone 5; a test recorder until then).
 *
 * <p>Contract, which every replica relies on to end up in the same state:
 * <ul>
 *   <li>{@link #apply} is called once per committed COMMAND entry, in log order, on the node's
 *       event loop.</li>
 *   <li>It must be <b>deterministic</b>: same commands in the same order give the same state and
 *       results on every replica. No clocks, no randomness, no {@code HashMap} iteration order.</li>
 * </ul>
 * In this milestone the state machine is volatile: after a restart Raft re-applies the log from
 * index 1. Snapshots (Milestone 8) add {@code snapshot()} and {@code restore()}.
 */
@FunctionalInterface
public interface StateMachine {

    /** Applies the command at {@code index} and returns the result for the waiting client. */
    Bytes apply(long index, Bytes command);
}
