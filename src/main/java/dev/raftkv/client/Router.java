package dev.raftkv.client;

import java.util.Optional;

import dev.raftkv.common.Bytes;

/**
 * Decides which group a request goes to. A single-group client always answers the same group;
 * the sharded client maps the key to its shard and the shard to its group with the cached
 * configuration. Called on the client's event loop.
 */
public interface Router {

    /** Where to send a request about {@code routingKey} now; empty if not known yet. */
    Optional<Route> route(Bytes routingKey);

    /**
     * The current view looks stale (a WRONG_GROUP reply, or no route): fetch a newer one in the
     * background. Requests are retried with backoff, so the next attempt uses whatever arrived.
     */
    default void refresh() {
    }

    /** A router for a client of exactly one group. */
    static Router fixed(Route route) {
        Optional<Route> r = Optional.of(route);
        return key -> r;
    }
}
