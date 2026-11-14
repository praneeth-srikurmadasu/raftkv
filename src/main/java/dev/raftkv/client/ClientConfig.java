package dev.raftkv.client;

import java.time.Duration;
import java.util.Objects;

/**
 * Client retry policy.
 *
 * @param attemptTimeout how long to wait for one reply before retrying (default 300 ms)
 * @param backoffBase    first retry backoff; doubles per attempt, with full jitter (default 20 ms)
 * @param backoffMax     largest backoff (default 1 s)
 * @param deadline       give up on an operation after this long (default 15 s)
 */
public record ClientConfig(Duration attemptTimeout, Duration backoffBase, Duration backoffMax, Duration deadline) {

    public ClientConfig {
        Objects.requireNonNull(attemptTimeout, "attemptTimeout");
        Objects.requireNonNull(backoffBase, "backoffBase");
        Objects.requireNonNull(backoffMax, "backoffMax");
        Objects.requireNonNull(deadline, "deadline");
        if (attemptTimeout.isZero() || attemptTimeout.isNegative() || deadline.compareTo(attemptTimeout) < 0) {
            throw new IllegalArgumentException("need 0 < attemptTimeout <= deadline");
        }
    }

    public static ClientConfig defaults() {
        return new ClientConfig(Duration.ofMillis(300), Duration.ofMillis(20), Duration.ofSeconds(1), Duration.ofSeconds(15));
    }
}
