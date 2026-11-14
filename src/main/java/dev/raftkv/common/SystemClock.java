package dev.raftkv.common;

/** The real monotonic clock, backed by {@link System#nanoTime()}. */
public enum SystemClock implements Clock {
    INSTANCE;

    @Override
    public long nanos() {
        return System.nanoTime();
    }
}
