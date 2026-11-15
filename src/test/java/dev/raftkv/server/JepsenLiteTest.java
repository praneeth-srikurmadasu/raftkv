package dev.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/**
 * A short Jepsen-lite run in every build (the nightly CI job runs a long one): real processes,
 * kill -9 and shard moves while clients run; the history must be linearizable.
 */
class JepsenLiteTest {

    @Test
    void realProcessesUnderKillsAndShardMovesStayLinearizable() {
        long seed = System.nanoTime();
        JepsenLite.Report r = JepsenLite.run(Duration.ofSeconds(15), seed);
        System.out.println("[jepsen-lite] seed " + seed + ": " + r.operations() + " operations, " + r.unknown()
                + " unknown, " + r.kills() + " kills, " + r.moves() + " moves");
        assertThat(r.linearizable()).as("seed %d: %s", seed, r.details()).isTrue();
        assertThat(r.operations()).as("the load really ran").isGreaterThan(100);
    }
}
