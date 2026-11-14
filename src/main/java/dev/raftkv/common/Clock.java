package dev.raftkv.common;

import java.util.concurrent.TimeUnit;

/**
 * A monotonic clock, injected everywhere instead of calling {@link System#nanoTime()} directly.
 *
 * <p>Production uses {@link SystemClock}; simulations use {@link ManualClock}, whose time only
 * moves when the simulation says so. That is what lets a 60-second chaos test run in
 * milliseconds and replay exactly (SPEC §3.2, determinism contract).
 *
 * <p>The value has no meaning on its own (it is not wall-clock time); only differences
 * between two readings of the same clock are meaningful.
 */
public interface Clock {

    /** Current time in nanoseconds. Never decreases. */
    long nanos();

    /** Current time in whole milliseconds. */
    default long millis() {
        return TimeUnit.NANOSECONDS.toMillis(nanos());
    }

    /**
     * Wall-clock time in milliseconds, comparable across machines (roughly: real clocks drift).
     * Used only where nodes must agree on a notion of "now", such as the timestamps a leader
     * stamps into log entries for session expiry. In a simulation all nodes share one clock.
     */
    default long wallMillis() {
        return millis();
    }
}
