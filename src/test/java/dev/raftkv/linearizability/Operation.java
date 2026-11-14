package dev.raftkv.linearizability;

import java.util.Objects;
import java.util.Optional;

import dev.raftkv.common.Bytes;
import dev.raftkv.kv.KVResult;
import dev.raftkv.sim.Simulation;

/**
 * One client operation as observed from outside the system: when it was invoked, when (if ever)
 * it returned, and what it returned.
 *
 * <p>An operation whose outcome is unknown (timed out, or still pending when the test ended)
 * has {@code returnNanos == Long.MAX_VALUE} and {@code result == null}: it may have taken effect
 * at any point after its invocation, or never.
 *
 * @param id          unique id within its history
 * @param client      which client issued it
 * @param kind        PUT, GET, DELETE or CAS
 * @param key         the key it touches
 * @param value       PUT value / CAS new value
 * @param expected    CAS expected value (empty = "only if absent")
 * @param invokeNanos when the client sent it
 * @param returnNanos when the client got the answer, or Long.MAX_VALUE if never
 * @param result      the answer, or null if unknown
 */
public record Operation(int id, int client, Kind kind, Bytes key, Optional<Bytes> value, Optional<Bytes> expected,
                        long invokeNanos, long returnNanos, KVResult result) {

    public enum Kind { PUT, GET, DELETE, CAS }

    public Operation {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(expected, "expected");
        if (returnNanos < invokeNanos) {
            throw new IllegalArgumentException("operation returned before it was invoked");
        }
        if ((result == null) != (returnNanos == Long.MAX_VALUE)) {
            throw new IllegalArgumentException("unknown operations have no result and no return time, and vice versa");
        }
    }

    public boolean isUnknown() {
        return result == null;
    }

    /** Human-readable line for failure reports. */
    public String describe() {
        String args = switch (kind) {
            case PUT -> key.toUtf8() + "=" + value.get().toUtf8();
            case GET, DELETE -> key.toUtf8();
            case CAS -> key.toUtf8() + " " + expected.map(Bytes::toUtf8).orElse("<absent>") + "->" + value.get().toUtf8();
        };
        String end = isUnknown() ? "?" : Simulation.formatTime(returnNanos);
        String out = isUnknown() ? "UNKNOWN" : describe(result);
        return String.format("#%-4d client %-2d [%s .. %s] %s %s -> %s", id, client, Simulation.formatTime(invokeNanos), end, kind, args, out);
    }

    private static String describe(KVResult r) {
        return switch (r) {
            case KVResult.PutResult p -> "prev=" + show(p.previous());
            case KVResult.GetResult g -> show(g.value());
            case KVResult.DeleteResult d -> d.existed() ? "deleted" : "absent";
            case KVResult.CasResult c -> (c.success() ? "ok" : "failed") + ", now " + show(c.current());
        };
    }

    private static String show(Optional<Bytes> v) {
        return v.map(Bytes::toUtf8).orElse("<absent>");
    }
}
