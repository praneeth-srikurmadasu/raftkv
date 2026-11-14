package dev.raftkv.common;

import java.time.Duration;

/**
 * A clock that only moves when told to. Used by the simulation and by unit tests.
 *
 * <p>Not thread-safe: in a simulation everything runs on one thread.
 */
public final class ManualClock implements Clock {

    private long nanos;

    public ManualClock() {
        this(0);
    }

    public ManualClock(long startNanos) {
        if (startNanos < 0) {
            throw new IllegalArgumentException("start time must be >= 0");
        }
        this.nanos = startNanos;
    }

    @Override
    public long nanos() {
        return nanos;
    }

    /** Moves time forward by {@code delta}. */
    public void advance(Duration delta) {
        if (delta.isNegative()) {
            throw new IllegalArgumentException("cannot move a clock backwards: " + delta);
        }
        nanos = Math.addExact(nanos, delta.toNanos());
    }

    /** Moves time forward to exactly {@code targetNanos}. */
    public void advanceTo(long targetNanos) {
        if (targetNanos < nanos) {
            throw new IllegalArgumentException(
                    "cannot move a clock backwards: now=" + nanos + " target=" + targetNanos);
        }
        nanos = targetNanos;
    }
}
