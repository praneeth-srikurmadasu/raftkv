package dev.raftkv.protocol;

/** Small argument checks shared by the message records. */
final class Checks {

    private Checks() {
    }

    static long nonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0, was " + value);
        }
        return value;
    }
}
