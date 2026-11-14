package dev.raftkv.sim;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks the seed for a randomized test.
 *
 * <p>Normally a fresh random seed every run (so CI keeps exploring new executions); when
 * {@code -Draftkv.seed=N} is set, exactly N (to replay a failure).
 */
public final class Seeds {

    public static final String PROPERTY = "raftkv.seed";

    private Seeds() {
    }

    /** The seed from {@code -Draftkv.seed}, or a new random one. */
    public static long resolve() {
        String value = System.getProperty(PROPERTY);
        if (value == null || value.isBlank()) {
            return ThreadLocalRandom.current().nextLong();
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("-D" + PROPERTY + " must be a long, was '" + value + "'", e);
        }
    }

    /** True when the user asked to replay one specific seed. */
    public static boolean isFixed() {
        String value = System.getProperty(PROPERTY);
        return value != null && !value.isBlank();
    }
}
