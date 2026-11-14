package dev.raftkv.common;

import java.time.Duration;
import java.util.concurrent.Executor;

/**
 * Runs tasks later, one at a time, on a single logical thread.
 *
 * <p>This is the "event loop" every Raft node runs on (SPEC §3.2). Production uses
 * {@link EventLoopScheduler} (a real thread); simulations use
 * {@code dev.raftkv.sim.Simulation}, which runs every node's tasks on one thread in a
 * seed-determined order.
 *
 * <p>Contract for implementations:
 * <ul>
 *   <li>Tasks never run concurrently with each other.</li>
 *   <li>A task scheduled with a smaller delay, at the same moment, never runs after one
 *       with a larger delay.</li>
 * </ul>
 * It extends {@link Executor} so it can be passed to {@code CompletableFuture} methods.
 */
public interface Scheduler extends Executor {

    /** Runs {@code task} after {@code delay} (zero means "as soon as possible"). */
    Cancellable schedule(Duration delay, Runnable task);

    /** Runs {@code task} as soon as possible. */
    @Override
    default void execute(Runnable task) {
        schedule(Duration.ZERO, task);
    }
}
