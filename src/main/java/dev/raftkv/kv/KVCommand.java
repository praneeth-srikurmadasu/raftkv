package dev.raftkv.kv;

import java.util.Objects;
import java.util.Optional;

import dev.raftkv.common.Bytes;

/**
 * An operation on the key-value store (SPEC §4.1). Keys and values are arbitrary bytes.
 *
 * <p>Every command, including {@link Get}, goes through the Raft log in this milestone ("log
 * reads": simple and linearizable). Milestone 11 adds ReadIndex and lease reads, which skip
 * the log for GETs.
 */
public sealed interface KVCommand {

    /** Largest key accepted. */
    int MAX_KEY_BYTES = 1024;
    /** Largest value accepted. */
    int MAX_VALUE_BYTES = 1024 * 1024;

    Bytes key();

    /** Sets {@code key} to {@code value}; the result holds the previous value. */
    record Put(Bytes key, Bytes value) implements KVCommand {
        public Put {
            checkKey(key);
            checkValue(value);
        }
    }

    /** Reads {@code key}. */
    record Get(Bytes key) implements KVCommand {
        public Get {
            checkKey(key);
        }
    }

    /** Removes {@code key}; the result says whether it existed. */
    record Delete(Bytes key) implements KVCommand {
        public Delete {
            checkKey(key);
        }
    }

    /**
     * Compare-and-set: sets {@code key} to {@code newValue} only if its current value equals
     * {@code expected} (an empty {@code expected} means "only if the key is absent").
     */
    record Cas(Bytes key, Optional<Bytes> expected, Bytes newValue) implements KVCommand {
        public Cas {
            checkKey(key);
            Objects.requireNonNull(expected, "expected");
            expected.ifPresent(KVCommand::checkValue);
            checkValue(newValue);
        }
    }

    private static void checkKey(Bytes key) {
        Objects.requireNonNull(key, "key");
        if (key.length() > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("key of " + key.length() + " bytes exceeds " + MAX_KEY_BYTES);
        }
    }

    private static void checkValue(Bytes value) {
        Objects.requireNonNull(value, "value");
        if (value.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("value of " + value.length() + " bytes exceeds " + MAX_VALUE_BYTES);
        }
    }
}
