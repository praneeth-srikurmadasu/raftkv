package dev.raftkv.common;

import java.nio.charset.StandardCharsets;

/**
 * Hand-written, stable 64-bit hashing (FNV-1a).
 *
 * <p>Used for the simulation's trace hash now and for {@code key -> shard} mapping later
 * (SPEC §6.1). We never use {@code Object.hashCode()} or {@code Arrays.hashCode()} for
 * anything that must be identical across machines and Java versions: those are 32-bit,
 * weakly mixed, and not specified as a stable cross-version contract for every type.
 *
 * <p>FNV-1a: start from an offset basis; for each byte, XOR it in, then multiply by a prime.
 */
public final class Hashing {

    public static final long FNV64_OFFSET_BASIS = 0xcbf29ce484222325L;
    public static final long FNV64_PRIME = 0x100000001b3L;

    private Hashing() {
    }

    /** Folds {@code b} into a running FNV-1a hash. */
    public static long fnv1a64(long hash, byte b) {
        return (hash ^ (b & 0xff)) * FNV64_PRIME;
    }

    /** Folds all of {@code data} into a running FNV-1a hash. */
    public static long fnv1a64(long hash, byte[] data) {
        long h = hash;
        for (byte b : data) {
            h = fnv1a64(h, b);
        }
        return h;
    }

    /** FNV-1a hash of {@code data}. */
    public static long fnv1a64(byte[] data) {
        return fnv1a64(FNV64_OFFSET_BASIS, data);
    }

    /** FNV-1a hash of the UTF-8 bytes of {@code text}. */
    public static long fnv1a64(String text) {
        return fnv1a64(text.getBytes(StandardCharsets.UTF_8));
    }
}
