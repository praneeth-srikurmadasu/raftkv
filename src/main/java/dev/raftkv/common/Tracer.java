package dev.raftkv.common;

/**
 * Receives one line per important event ("became leader", "granted vote to n3", ...).
 *
 * <p>In simulations the {@code Simulation} itself is the tracer, so Raft events become part
 * of the run's fingerprint and of the failure report. In production it will feed the
 * structured log.
 */
@FunctionalInterface
public interface Tracer {

    /** Discards everything. */
    Tracer NOOP = event -> { };

    void trace(String event);
}
