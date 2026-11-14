package dev.raftkv.sim;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.BooleanSupplier;

import dev.raftkv.common.Cancellable;
import dev.raftkv.common.Clock;
import dev.raftkv.common.Hashing;
import dev.raftkv.common.ManualClock;
import dev.raftkv.common.Scheduler;

/**
 * A deterministic discrete-event simulator: the heart of RaftKV's test strategy (SPEC §3.2, §9.2).
 *
 * <p>Instead of real threads and real time, every node, timer and network message in a test
 * cluster schedules its work here. The simulation keeps one priority queue of events ordered
 * by virtual time and runs them one at a time on the calling thread, jumping the clock
 * straight to the next event. Consequences:
 * <ul>
 *   <li><b>Fast:</b> a 60-second scenario with no real waiting runs in milliseconds.</li>
 *   <li><b>Reproducible:</b> all randomness comes from one seed, and ties between events at the
 *       same instant are broken by that seed too, so the same seed gives the same execution,
 *       event for event. Any failure can be replayed with {@code -Draftkv.seed=...}.</li>
 *   <li><b>Checkable:</b> components call {@link #trace(String)} for important events; the
 *       running {@link #traceHash()} lets a test prove two runs were identical.</li>
 * </ul>
 * This is the approach FoundationDB made famous ("simulation testing").
 *
 * <p>Not thread-safe by design: everything runs on one thread.
 */
public final class Simulation implements Scheduler {

    private static final int RECENT_TRACE_LIMIT = 200;

    private static final Comparator<Event> ORDER = Comparator
            .comparingLong((Event e) -> e.time)
            .thenComparingLong(e -> e.tieBreaker)
            .thenComparingLong(e -> e.sequence);

    private final long seed;
    private final Random random;
    private final ManualClock clock = new ManualClock();
    private final PriorityQueue<Event> queue = new PriorityQueue<>(ORDER);
    private final ArrayDeque<String> recentTrace = new ArrayDeque<>();
    private long nextSequence;
    private long eventsRun;
    private long traceHash = Hashing.FNV64_OFFSET_BASIS;

    public Simulation(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
    }

    /** The seed this simulation was created with. Print it whenever a test fails. */
    public long seed() {
        return seed;
    }

    /** The virtual clock. Only {@link #step()} and the run methods move it. */
    public Clock clock() {
        return clock;
    }

    /** Current virtual time in nanoseconds. */
    public long now() {
        return clock.nanos();
    }

    /** The simulation's master random source. */
    public Random random() {
        return random;
    }

    /**
     * A new independent random source derived from the master seed. Give each component
     * (network, each node's election timer...) its own, so their random choices stay
     * separate streams.
     */
    public Random forkRandom() {
        return new Random(random.nextLong());
    }

    @Override
    public Cancellable schedule(Duration delay, Runnable task) {
        Objects.requireNonNull(task, "task");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("negative delay: " + delay);
        }
        Event event = new Event(Math.addExact(clock.nanos(), delay.toNanos()), random.nextLong(), nextSequence++, task);
        queue.add(event);
        return () -> {
            if (event.cancelled || event.ran) {
                return false;
            }
            event.cancelled = true; // removed lazily when it reaches the head of the queue
            return true;
        };
    }

    /**
     * Runs the next event, advancing the clock to its time.
     *
     * @return false if there was nothing left to run
     */
    public boolean step() {
        Event event = pollLive();
        if (event == null) {
            return false;
        }
        clock.advanceTo(event.time);
        event.ran = true;
        eventsRun++;
        try {
            event.task.run();
        } catch (RuntimeException | Error e) {
            throw new SimulationException("event failed at t=" + formatTime(clock.nanos())
                    + " (re-run with -Draftkv.seed=" + seed + ")", e);
        }
        return true;
    }

    /** Runs every event due within {@code duration} from now, then sets the clock to exactly now + duration. */
    public void runFor(Duration duration) {
        long end = Math.addExact(clock.nanos(), duration.toNanos());
        while (true) {
            Event next = peekLive();
            if (next == null || next.time > end) {
                break;
            }
            step();
        }
        clock.advanceTo(end);
    }

    /**
     * Runs events until {@code condition} is true or {@code maxDuration} of virtual time passes.
     * The condition is checked before the first event and after every event.
     *
     * @return true if the condition became true
     */
    public boolean runUntil(BooleanSupplier condition, Duration maxDuration) {
        long end = Math.addExact(clock.nanos(), maxDuration.toNanos());
        while (!condition.getAsBoolean()) {
            Event next = peekLive();
            if (next == null || next.time > end) {
                clock.advanceTo(Math.max(end, clock.nanos()));
                return false;
            }
            step();
        }
        return true;
    }

    /**
     * Records an event in the trace. Folded into {@link #traceHash()} together with the current
     * virtual time; the most recent entries are kept for failure reports.
     */
    public void trace(String event) {
        String line = clock.nanos() + " " + event;
        traceHash = Hashing.fnv1a64(traceHash, line.getBytes(StandardCharsets.UTF_8));
        traceHash = Hashing.fnv1a64(traceHash, (byte) '\n');
        if (recentTrace.size() == RECENT_TRACE_LIMIT) {
            recentTrace.removeFirst();
        }
        recentTrace.addLast(formatTime(clock.nanos()) + " " + event);
    }

    /** Hash of every traced event so far. Same seed + same code = same hash. */
    public long traceHash() {
        return traceHash;
    }

    /** The last few traced events, oldest first, with readable timestamps. */
    public List<String> recentTrace() {
        return new ArrayList<>(recentTrace);
    }

    /** Number of events executed so far. */
    public long eventsRun() {
        return eventsRun;
    }

    /** Number of scheduled events not yet run (including cancelled ones not yet discarded). */
    public int pendingEvents() {
        return queue.size();
    }

    /** Formats virtual nanoseconds as seconds with millisecond precision, e.g. {@code 12.345s}. */
    public static String formatTime(long nanos) {
        return String.format("%d.%03ds", nanos / 1_000_000_000L, (nanos / 1_000_000L) % 1000);
    }

    private Event pollLive() {
        Event e = queue.poll();
        while (e != null && e.cancelled) {
            e = queue.poll();
        }
        return e;
    }

    private Event peekLive() {
        Event e = queue.peek();
        while (e != null && e.cancelled) {
            queue.poll();
            e = queue.peek();
        }
        return e;
    }

    private static final class Event {
        final long time;
        final long tieBreaker;
        final long sequence;
        final Runnable task;
        boolean cancelled;
        boolean ran;

        Event(long time, long tieBreaker, long sequence, Runnable task) {
            this.time = time;
            this.tieBreaker = tieBreaker;
            this.sequence = sequence;
            this.task = task;
        }
    }
}
