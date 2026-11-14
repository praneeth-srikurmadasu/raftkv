package dev.raftkv.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HashingTest {

    /** Official FNV-1a 64-bit test vectors (from the FNV reference test suite). */
    @Test
    void matchesReferenceVectors() {
        assertThat(Hashing.fnv1a64("")).isEqualTo(0xcbf29ce484222325L);
        assertThat(Hashing.fnv1a64("a")).isEqualTo(0xaf63dc4c8601ec8cL);
        assertThat(Hashing.fnv1a64("foobar")).isEqualTo(0x85944171f73967e8L);
    }

    @Test
    void incrementalEqualsOneShot() {
        long h = Hashing.FNV64_OFFSET_BASIS;
        for (byte b : "foobar".getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
            h = Hashing.fnv1a64(h, b);
        }
        assertThat(h).isEqualTo(Hashing.fnv1a64("foobar"));
    }
}
