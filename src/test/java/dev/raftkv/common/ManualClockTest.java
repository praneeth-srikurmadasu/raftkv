package dev.raftkv.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class ManualClockTest {

    @Test
    void movesOnlyWhenTold() {
        ManualClock clock = new ManualClock();
        assertThat(clock.nanos()).isEqualTo(0L);
        clock.advance(Duration.ofMillis(5));
        assertThat(clock.millis()).isEqualTo(5L);
        clock.advanceTo(Duration.ofSeconds(1).toNanos());
        assertThat(clock.millis()).isEqualTo(1000L);
    }

    @Test
    void neverMovesBackwards() {
        ManualClock clock = new ManualClock(100);
        assertThatThrownBy(() -> clock.advanceTo(99)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> clock.advance(Duration.ofNanos(-1))).isInstanceOf(IllegalArgumentException.class);
        assertThat(clock.nanos()).isEqualTo(100L);
    }
}
