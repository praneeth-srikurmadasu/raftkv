package dev.raftkv.common;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A real event loop: one dedicated platform thread runs every task, in order.
 *
 * <p>Why one thread? All Raft state of a node is touched only from this thread, so the Raft
 * core needs no locks, and the order of events is easy to reason about (SPEC §3.2).
 * Slow work (fsync, network writes) runs elsewhere and posts its result back here.
 *
 * <p>An exception thrown by a task is passed to the error handler instead of being
 * swallowed (a plain {@code ScheduledExecutorService} would hide it in a future nobody reads).
 */
public final class EventLoopScheduler implements Scheduler, AutoCloseable {

    private final ScheduledThreadPoolExecutor executor;
    private final Consumer<Throwable> errorHandler;
    private volatile Thread loopThread;

    public EventLoopScheduler(String threadName) {
        this(threadName, error -> {
            System.err.println("Uncaught error in event loop " + threadName);
            error.printStackTrace();
        });
    }

    public EventLoopScheduler(String threadName, Consumer<Throwable> errorHandler) {
        Objects.requireNonNull(threadName, "threadName");
        this.errorHandler = Objects.requireNonNull(errorHandler, "errorHandler");
        this.executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread t = new Thread(runnable, threadName);
            t.setDaemon(true);
            loopThread = t;
            return t;
        });
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        Objects.requireNonNull(task, "task");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("negative delay: " + delay);
        }
        ScheduledFuture<?> future = executor.schedule(() -> runSafely(task), delay.toNanos(), TimeUnit.NANOSECONDS);
        return () -> future.cancel(false);
    }

    /** True when called from this loop's own thread. */
    public boolean inEventLoop() {
        return Thread.currentThread() == loopThread;
    }

    private void runSafely(Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            errorHandler.accept(t);
        }
    }

    /** Stops the loop; pending tasks are discarded. Waits up to 5 seconds for a running task. */
    @Override
    public void close() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                errorHandler.accept(new IllegalStateException("event loop did not stop within 5s"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
