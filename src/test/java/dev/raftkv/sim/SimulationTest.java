package dev.raftkv.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dev.raftkv.common.Cancellable;
import org.junit.jupiter.api.Test;

class SimulationTest {

    @Test
    void runsEventsInTimeOrderAndJumpsTheClock() {
        Simulation sim = new Simulation(1);
        List<String> log = new ArrayList<>();
        sim.schedule(Duration.ofMillis(30), () -> log.add("c@" + sim.clock().millis()));
        sim.schedule(Duration.ofMillis(10), () -> log.add("a@" + sim.clock().millis()));
        sim.schedule(Duration.ofMillis(20), () -> log.add("b@" + sim.clock().millis()));

        while (sim.step()) {
            // run everything
        }
        assertThat(log).isEqualTo(List.of("a@10", "b@20", "c@30"));
        assertThat(sim.eventsRun()).isEqualTo(3L);
    }

    @Test
    void tasksCanScheduleMoreTasks() {
        Simulation sim = new Simulation(1);
        List<Long> times = new ArrayList<>();
        Runnable[] tick = new Runnable[1];
        tick[0] = () -> {
            times.add(sim.clock().millis());
            if (times.size() < 5) {
                sim.schedule(Duration.ofMillis(50), tick[0]); // like a heartbeat timer
            }
        };
        sim.execute(tick[0]);
        sim.runFor(Duration.ofSeconds(1));
        assertThat(times).isEqualTo(List.of(0L, 50L, 100L, 150L, 200L));
        assertThat(sim.clock().millis()).isEqualTo(1000L); // runFor ends exactly at now + duration
    }

    @Test
    void runForStopsAtTheDeadline() {
        Simulation sim = new Simulation(1);
        List<String> log = new ArrayList<>();
        sim.schedule(Duration.ofMillis(100), () -> log.add("in"));
        sim.schedule(Duration.ofMillis(101), () -> log.add("out"));
        sim.runFor(Duration.ofMillis(100));
        assertThat(log).isEqualTo(List.of("in"));
        assertThat(sim.pendingEvents()).isEqualTo(1);
    }

    @Test
    void cancelledEventsNeverRun() {
        Simulation sim = new Simulation(1);
        List<String> log = new ArrayList<>();
        Cancellable c = sim.schedule(Duration.ofMillis(5), () -> log.add("cancelled"));
        Cancellable done = sim.schedule(Duration.ofMillis(1), () -> log.add("ran"));
        assertThat(c.cancel()).isTrue();
        assertThat(c.cancel()).isFalse(); // already cancelled
        sim.runFor(Duration.ofMillis(10));
        assertThat(log).isEqualTo(List.of("ran"));
        assertThat(done.cancel()).isFalse(); // already ran
    }

    @Test
    void runUntilReportsWhetherTheConditionWasMet() {
        Simulation sim = new Simulation(1);
        int[] counter = {0};
        for (int i = 1; i <= 10; i++) {
            sim.schedule(Duration.ofMillis(i * 10L), () -> counter[0]++);
        }
        assertThat(sim.runUntil(() -> counter[0] == 3, Duration.ofSeconds(1))).isTrue();
        assertThat(sim.clock().millis()).isEqualTo(30L);

        assertThat(sim.runUntil(() -> counter[0] == 100, Duration.ofMillis(500))).isFalse();
        assertThat(sim.clock().millis()).isEqualTo(530L); // gave up at the deadline
    }

    /** Events at the same instant run in an order chosen by the seed: fixed per seed, varied across seeds. */
    @Test
    void simultaneousEventsAreOrderedBySeed() {
        assertThat(orderOfSimultaneousEvents(5)).isEqualTo(orderOfSimultaneousEvents(5));
        Set<List<Integer>> orders = new HashSet<>();
        for (long seed = 0; seed < 20; seed++) {
            orders.add(orderOfSimultaneousEvents(seed));
        }
        assertThat(orders.size()).isGreaterThan(1);
    }

    @Test
    void failuresReportTheSeedAndTime() {
        Simulation sim = new Simulation(8812734);
        sim.schedule(Duration.ofMillis(1500), () -> {
            throw new IllegalStateException("invariant broken");
        });
        assertThatThrownBy(sim::step)
                .isInstanceOf(SimulationException.class)
                .hasMessageContaining("-Draftkv.seed=8812734")
                .hasMessageContaining("t=1.500s");
    }

    @Test
    void traceHashChangesWhenAnyEventChanges() {
        Simulation a = new Simulation(1);
        Simulation b = new Simulation(1);
        a.trace("x");
        b.trace("x");
        assertThat(a.traceHash()).isEqualTo(b.traceHash());
        a.trace("y");
        b.trace("z");
        assertThat(a.traceHash()).isNotEqualTo(b.traceHash());
        assertThat(a.recentTrace()).isEqualTo(List.of("0.000s x", "0.000s y"));
    }

    @Test
    void rejectsNegativeDelays() {
        assertThatThrownBy(() -> new Simulation(1).schedule(Duration.ofMillis(-1), () -> { }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static List<Integer> orderOfSimultaneousEvents(long seed) {
        Simulation sim = new Simulation(seed);
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            int id = i;
            sim.schedule(Duration.ofMillis(10), () -> order.add(id));
        }
        sim.runFor(Duration.ofMillis(10));
        return order;
    }
}
