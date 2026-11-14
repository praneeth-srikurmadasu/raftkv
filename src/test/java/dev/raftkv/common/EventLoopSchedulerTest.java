package dev.raftkv.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

/** Uses real time, so it only checks ordering and threading, never exact timings. */
class EventLoopSchedulerTest {

    @Test
    void runsTasksInOrderOnOneThread() throws InterruptedException {
        List<String> order = new CopyOnWriteArrayList<>();
        List<String> threads = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(3);
        try (EventLoopScheduler loop = new EventLoopScheduler("test-loop")) {
            loop.schedule(Duration.ofMillis(500), () -> { order.add("late"); threads.add(Thread.currentThread().getName()); done.countDown(); });
            loop.execute(() -> { order.add("first"); threads.add(Thread.currentThread().getName()); done.countDown(); });
            loop.execute(() -> { order.add("second"); threads.add(Thread.currentThread().getName()); done.countDown(); });
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(order).isEqualTo(List.of("first", "second", "late"));
        assertThat(threads).isEqualTo(List.of("test-loop", "test-loop", "test-loop"));
    }

    @Test
    void cancelledTaskNeverRuns() throws InterruptedException {
        AtomicBoolean ran = new AtomicBoolean();
        CountDownLatch marker = new CountDownLatch(1);
        try (EventLoopScheduler loop = new EventLoopScheduler("test-loop")) {
            Cancellable c = loop.schedule(Duration.ofMillis(50), () -> ran.set(true));
            assertThat(c.cancel()).isTrue();
            loop.schedule(Duration.ofMillis(100), marker::countDown);
            assertThat(marker.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(ran.get()).isFalse();
    }

    @Test
    void errorsGoToTheHandlerAndTheLoopKeepsRunning() throws InterruptedException {
        AtomicReference<Throwable> seen = new AtomicReference<>();
        CountDownLatch after = new CountDownLatch(1);
        try (EventLoopScheduler loop = new EventLoopScheduler("test-loop", seen::set)) {
            loop.execute(() -> { throw new IllegalStateException("boom"); });
            loop.execute(after::countDown);
            assertThat(after.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(seen.get()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void knowsWhetherCallerIsOnTheLoop() throws InterruptedException {
        AtomicBoolean inside = new AtomicBoolean();
        CountDownLatch done = new CountDownLatch(1);
        try (EventLoopScheduler loop = new EventLoopScheduler("test-loop")) {
            loop.execute(() -> { inside.set(loop.inEventLoop()); done.countDown(); });
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(loop.inEventLoop()).isFalse();
        }
        assertThat(inside.get()).isTrue();
    }
}
