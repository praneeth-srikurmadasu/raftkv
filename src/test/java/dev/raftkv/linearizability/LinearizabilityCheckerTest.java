package dev.raftkv.linearizability;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import dev.raftkv.common.Bytes;
import dev.raftkv.kv.KVResult;
import org.junit.jupiter.api.Test;

/**
 * The checker's own tests. A checker that always says "linearizable" would make every chaos
 * run pass, so most of these build histories that MUST be rejected.
 */
class LinearizabilityCheckerTest {

    private static final Bytes K = Bytes.ofUtf8("k");
    private static final long NEVER = Long.MAX_VALUE;

    private final List<Operation> ops = new ArrayList<>();

    private static Optional<Bytes> v(String s) {
        return s == null ? Optional.empty() : Optional.of(Bytes.ofUtf8(s));
    }

    private void put(int client, long from, long to, String value, String previous) {
        ops.add(new Operation(ops.size(), client, Operation.Kind.PUT, K, v(value), Optional.empty(), from, to,
                to == NEVER ? null : new KVResult.PutResult(v(previous))));
    }

    private void get(int client, long from, long to, String seen) {
        ops.add(new Operation(ops.size(), client, Operation.Kind.GET, K, Optional.empty(), Optional.empty(), from, to,
                to == NEVER ? null : new KVResult.GetResult(v(seen))));
    }

    private void cas(int client, long from, long to, String expected, String value, boolean success, String current) {
        ops.add(new Operation(ops.size(), client, Operation.Kind.CAS, K, v(value), v(expected), from, to,
                to == NEVER ? null : new KVResult.CasResult(success, v(current))));
    }

    private void delete(int client, long from, long to, boolean existed) {
        ops.add(new Operation(ops.size(), client, Operation.Kind.DELETE, K, Optional.empty(), Optional.empty(), from, to,
                to == NEVER ? null : new KVResult.DeleteResult(existed)));
    }

    private boolean linearizable() {
        return LinearizabilityChecker.check(ops).linearizable();
    }

    // ------------------------------------------------------------------ must accept

    @Test
    void sequentialHistoryIsLinearizable() {
        put(1, 0, 10, "a", null);
        get(1, 20, 30, "a");
        put(2, 40, 50, "b", "a");
        delete(1, 60, 70, true);
        get(2, 80, 90, null);
        assertThat(linearizable()).isTrue();
    }

    @Test
    void concurrentOperationsMayTakeEffectInEitherOrder() {
        put(1, 0, 100, "a", null);     // overlaps the read
        get(2, 10, 50, "a");           // sees it: fine, the put can take effect at t=10
        assertThat(linearizable()).isTrue();
        ops.clear();
        put(1, 0, 100, "a", null);
        get(2, 10, 50, null);          // doesn't see it: also fine, the put can take effect later
        assertThat(linearizable()).isTrue();
    }

    @Test
    void anUnknownWriteMayExplainALaterRead() {
        put(1, 0, NEVER, "a", null);   // timed out: did it happen?
        get(2, 50, 60, "a");           // yes, apparently
        assertThat(linearizable()).isTrue();
    }

    @Test
    void anUnknownWriteMayAlsoNeverHappen() {
        put(1, 0, NEVER, "a", null);
        get(2, 50, 60, null);
        get(2, 70, 80, null);
        assertThat(linearizable()).isTrue();
    }

    @Test
    void casRaceWithOneWinner() {
        put(1, 0, 10, "0", null);
        cas(1, 20, 100, "0", "1", true, "1");
        cas(2, 20, 100, "0", "2", false, "1");
        assertThat(linearizable()).isTrue();
    }

    // ------------------------------------------------------------------ must reject

    @Test
    void rejectsAStaleRead() {
        put(1, 0, 10, "a", null);      // completed ...
        get(2, 20, 30, null);          // ... strictly before this read began: the read must see it
        LinearizabilityChecker.Result r = LinearizabilityChecker.check(ops);
        assertThat(r.linearizable()).isFalse();
        assertThat(r.report()).contains("NOT linearizable");
    }

    @Test
    void rejectsALostWrite() {
        put(1, 0, 10, "a", null);
        put(1, 20, 30, "b", "a");
        get(2, 40, 50, "a");           // b was acknowledged before this read started
        assertThat(linearizable()).isFalse();
    }

    @Test
    void rejectsTwoCasWinners() {
        put(1, 0, 10, "0", null);
        cas(1, 20, 100, "0", "1", true, "1");
        cas(2, 20, 100, "0", "2", true, "2"); // both claim to have replaced "0"
        assertThat(linearizable()).isFalse();
    }

    @Test
    void rejectsAValueNobodyWrote() {
        put(1, 0, 10, "a", null);
        get(2, 20, 30, "zzz");
        assertThat(linearizable()).isFalse();
    }

    @Test
    void rejectsAWriteThatAppliedTwice() {
        put(1, 0, 10, "a", null);
        put(2, 20, 30, "b", "a");
        put(1, 40, 50, "c", "b");
        get(2, 60, 70, "c");
        put(2, 80, 90, "d", "a");      // "a" again: only possible if the first write re-executed
        assertThat(linearizable()).isFalse();
    }

    @Test
    void rejectsReadsGoingBackInTime() {
        put(1, 0, 100, "a", null);     // concurrent with both reads
        get(2, 10, 20, "a");           // the first read sees the new value...
        get(3, 30, 40, null);          // ...a later read must not see the old one again
        assertThat(linearizable()).isFalse();
    }

    @Test
    void rejectsAWrongDeleteAnswer() {
        put(1, 0, 10, "a", null);
        delete(2, 20, 30, false);      // says the key wasn't there
        assertThat(linearizable()).isFalse();
    }

    // ------------------------------------------------------------------ scale

    /**
     * A large, genuinely linearizable history (generated by executing operations one at a time
     * at random instants inside overlapping intervals) must be accepted, quickly.
     */
    @Test
    void acceptsALargeConcurrentLinearizableHistory() {
        Random r = new Random(5);
        Optional<Bytes> state = Optional.empty();
        long t = 0;
        for (int i = 0; i < 400; i++) {
            t += 10;
            long invoke = t - r.nextInt(25);     // overlapping intervals around the effect point t
            long ret = t + r.nextInt(25);
            int client = r.nextInt(5);
            String val = "v" + i;
            switch (r.nextInt(4)) {
                case 0 -> {
                    ops.add(new Operation(ops.size(), client, Operation.Kind.PUT, K, v(val), Optional.empty(), invoke, ret,
                            new KVResult.PutResult(state)));
                    state = v(val);
                }
                case 1 -> ops.add(new Operation(ops.size(), client, Operation.Kind.GET, K, Optional.empty(), Optional.empty(),
                        invoke, ret, new KVResult.GetResult(state)));
                case 2 -> {
                    ops.add(new Operation(ops.size(), client, Operation.Kind.DELETE, K, Optional.empty(), Optional.empty(),
                            invoke, ret, new KVResult.DeleteResult(state.isPresent())));
                    state = Optional.empty();
                }
                default -> {
                    Optional<Bytes> expected = r.nextBoolean() ? state : v("other");
                    boolean ok = expected.equals(state);
                    Optional<Bytes> after = ok ? v(val) : state;
                    ops.add(new Operation(ops.size(), client, Operation.Kind.CAS, K, v(val), expected, invoke, ret,
                            new KVResult.CasResult(ok, after)));
                    state = after;
                }
            }
        }
        long start = System.nanoTime();
        assertThat(linearizable()).isTrue();
        assertThat((System.nanoTime() - start) / 1_000_000).as("milliseconds").isLessThan(5_000L);
    }

    @Test
    void keysAreCheckedIndependently() {
        ops.add(new Operation(0, 1, Operation.Kind.PUT, Bytes.ofUtf8("x"), v("1"), Optional.empty(), 0, 10, new KVResult.PutResult(v(null))));
        ops.add(new Operation(1, 2, Operation.Kind.GET, Bytes.ofUtf8("y"), Optional.empty(), Optional.empty(), 20, 30, new KVResult.GetResult(v(null))));
        assertThat(linearizable()).isTrue(); // y was never written, so absent is right
    }
}
