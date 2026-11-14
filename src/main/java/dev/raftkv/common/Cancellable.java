package dev.raftkv.common;

/** Handle for a scheduled task. */
@FunctionalInterface
public interface Cancellable {

    /**
     * Prevents the task from running if it has not started yet.
     *
     * @return {@code true} if this call stopped the task; {@code false} if it already ran,
     *         is running, or was already cancelled
     */
    boolean cancel();
}
