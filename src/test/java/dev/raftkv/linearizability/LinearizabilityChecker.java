package dev.raftkv.linearizability;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import dev.raftkv.common.Bytes;
import dev.raftkv.kv.KVResult;

/**
 * Decides whether a history of KV operations is <b>linearizable</b> (SPEC §9.4).
 *
 * <p>Linearizable means: there is a single sequential order of all operations such that
 * (1) every operation takes effect at one instant between its invocation and its return, so
 * an operation that returned before another was invoked comes first, and (2) every answer is
 * what a single, correct, non-replicated map would have answered in that order.
 *
 * <h2>Per-key checking</h2>
 * Linearizability is <em>local</em> (Herlihy &amp; Wing, 1990): a history is linearizable iff the
 * sub-history of each object is. Every KV operation touches exactly one key, so each key is
 * checked on its own. That turns one huge search into many small ones.
 *
 * <h2>The search (Wing &amp; Gong, with Lowe's memoization; the Porcupine algorithm)</h2>
 * Invocations and returns become events in time order, in a linked list. Walking it:
 * <ul>
 *   <li>at an <b>invocation</b>, try to make that operation take effect now: if the model accepts
 *       its answer, remove it from the list ("linearize" it), push a backtrack point, and start over
 *       from the beginning;</li>
 *   <li>at a <b>return</b> whose operation has not been linearized, the current guess is impossible
 *       (the operation must take effect before it returns), so backtrack: undo the last choice
 *       and try the next candidate;</li>
 *   <li>if the list empties, a valid order exists; if the backtrack stack empties, none does.</li>
 * </ul>
 * Memoization: the pair (set of linearized operations, model state) is cached; reaching the same
 * pair again cannot lead anywhere new. This turns most exponential searches into fast ones.
 *
 * <p>Operations with an unknown outcome get a return time of infinity and accept any answer,
 * so the search may place them anywhere after their invocation (including at the very end,
 * which is the same as "never happened" for everything observable).
 */
public final class LinearizabilityChecker {

    /** Stops runaway searches; histories in our tests need far fewer steps. */
    private static final long STEP_BUDGET = 20_000_000L;

    /** Outcome of checking one history. */
    public record Result(boolean linearizable, boolean inconclusive, String key, String report) {
        static Result ok() {
            return new Result(true, false, null, "linearizable");
        }
    }

    private LinearizabilityChecker() {
    }

    /** Checks a whole history, key by key. */
    public static Result check(List<Operation> history) {
        Map<Bytes, List<Operation>> byKey = new TreeMap<>();
        for (Operation op : history) {
            if (op.isUnknown() && op.kind() == Operation.Kind.GET) {
                continue; // an unanswered read changes nothing and constrains nothing
            }
            byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }
        for (Map.Entry<Bytes, List<Operation>> e : byKey.entrySet()) {
            Result r = checkKey(e.getKey(), e.getValue());
            if (!r.linearizable()) {
                return r;
            }
        }
        return Result.ok();
    }

    // ------------------------------------------------------------------ the search

    private static final class Entry {
        final int op;          // index into ops
        final boolean isCall;
        final long time;
        Entry match;
        Entry prev;
        Entry next;

        Entry(int op, boolean isCall, long time) {
            this.op = op;
            this.isCall = isCall;
            this.time = time;
        }
    }

    private record Frame(Entry call, Optional<Bytes> stateBefore) {}

    private record CacheKey(BitSet linearized, Optional<Bytes> state) {}

    static Result checkKey(Bytes key, List<Operation> ops) {
        List<Entry> events = new ArrayList<>(ops.size() * 2);
        for (int i = 0; i < ops.size(); i++) {
            Operation op = ops.get(i);
            Entry call = new Entry(i, true, op.invokeNanos());
            Entry ret = new Entry(i, false, op.returnNanos());
            call.match = ret;
            ret.match = call;
            events.add(call);
            events.add(ret);
        }
        // Time order; at equal times, invocations first (treat touching operations as concurrent).
        events.sort(Comparator.comparingLong((Entry e) -> e.time).thenComparing(e -> !e.isCall));
        Entry head = new Entry(-1, false, Long.MIN_VALUE);
        Entry tail = head;
        for (Entry e : events) {
            tail.next = e;
            e.prev = tail;
            tail = e;
        }

        Optional<Bytes> state = Optional.empty(); // every key starts absent
        BitSet linearized = new BitSet(ops.size());
        Set<CacheKey> cache = new HashSet<>();
        Deque<Frame> stack = new ArrayDeque<>();
        Entry entry = head.next;
        long steps = 0;
        while (head.next != null) {
            if (++steps > STEP_BUDGET) {
                return new Result(false, true, key.toUtf8(), "search budget exhausted for key " + key.toUtf8());
            }
            if (entry.isCall) {
                Optional<Optional<Bytes>> next = step(state, ops.get(entry.op));
                if (next.isPresent()) {
                    BitSet candidate = (BitSet) linearized.clone();
                    candidate.set(entry.op);
                    if (cache.add(new CacheKey(candidate, next.get()))) {
                        stack.push(new Frame(entry, state));
                        state = next.get();
                        linearized.set(entry.op);
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                }
                entry = entry.next;
            } else {
                if (stack.isEmpty()) {
                    return new Result(false, false, key.toUtf8(), report(key, ops, entry.op));
                }
                Frame f = stack.pop();
                state = f.stateBefore();
                linearized.clear(f.call().op);
                unlift(f.call());
                entry = f.call().next;
            }
        }
        return Result.ok();
    }

    private static void lift(Entry call) {
        call.prev.next = call.next;
        call.next.prev = call.prev;
        Entry ret = call.match;
        ret.prev.next = ret.next;
        if (ret.next != null) {
            ret.next.prev = ret.prev;
        }
    }

    private static void unlift(Entry call) {
        Entry ret = call.match;
        ret.prev.next = ret;
        if (ret.next != null) {
            ret.next.prev = ret;
        }
        call.prev.next = call;
        call.next.prev = call;
    }

    // ------------------------------------------------------------------ the sequential model

    /**
     * The single-copy map for one key: given the current value, does this operation's answer make
     * sense, and what is the value afterwards? Empty = the answer is impossible in this state.
     */
    static Optional<Optional<Bytes>> step(Optional<Bytes> state, Operation op) {
        KVResult r = op.result();
        switch (op.kind()) {
            case PUT -> {
                if (r != null && !((KVResult.PutResult) r).previous().equals(state)) {
                    return Optional.empty();
                }
                return Optional.of(op.value());
            }
            case GET -> {
                if (r != null && !((KVResult.GetResult) r).value().equals(state)) {
                    return Optional.empty();
                }
                return Optional.of(state);
            }
            case DELETE -> {
                if (r != null && ((KVResult.DeleteResult) r).existed() != state.isPresent()) {
                    return Optional.empty();
                }
                return Optional.of(Optional.empty());
            }
            case CAS -> {
                boolean matches = state.equals(op.expected());
                Optional<Bytes> after = matches ? op.value() : state;
                if (r != null) {
                    KVResult.CasResult c = (KVResult.CasResult) r;
                    if (c.success() != matches || !c.current().equals(after)) {
                        return Optional.empty();
                    }
                }
                return Optional.of(after);
            }
        }
        throw new AssertionError("unreachable");
    }

    // ------------------------------------------------------------------ reporting

    private static String report(Bytes key, List<Operation> ops, int stuckOp) {
        StringBuilder sb = new StringBuilder();
        sb.append("NOT linearizable: key '").append(key.toUtf8()).append("' (").append(ops.size())
                .append(" operations). No valid order lets #").append(ops.get(stuckOp).id())
                .append(" take effect before it returned. History of this key:\n");
        ops.stream().sorted(Comparator.comparingLong(Operation::invokeNanos))
                .forEach(op -> sb.append("  ").append(op.describe()).append('\n'));
        return sb.toString();
    }
}
