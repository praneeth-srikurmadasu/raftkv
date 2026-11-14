package dev.raftkv.linearizability;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

import dev.raftkv.common.Bytes;
import dev.raftkv.kv.KVResult;

/**
 * Records operations as clients invoke them and as answers arrive (SPEC §9.4). Times come from
 * the simulation's virtual clock, so a history is exactly reproducible from its seed.
 */
public final class History {

    private final LongSupplier clock;
    private final List<Pending> all = new ArrayList<>();

    private static final class Pending {
        final int id;
        final int client;
        final Operation.Kind kind;
        final Bytes key;
        final Optional<Bytes> value;
        final Optional<Bytes> expected;
        final long invoke;
        long ret = Long.MAX_VALUE;
        KVResult result;

        Pending(int id, int client, Operation.Kind kind, Bytes key, Optional<Bytes> value, Optional<Bytes> expected, long invoke) {
            this.id = id;
            this.client = client;
            this.kind = kind;
            this.key = key;
            this.value = value;
            this.expected = expected;
            this.invoke = invoke;
        }
    }

    public History(LongSupplier clock) {
        this.clock = clock;
    }

    /** Records an invocation; returns a handle for {@link #ok}. */
    public int invoke(int client, Operation.Kind kind, Bytes key, Optional<Bytes> value, Optional<Bytes> expected) {
        int id = all.size();
        all.add(new Pending(id, client, kind, key, value, expected, clock.getAsLong()));
        return id;
    }

    /** Records a successful answer. (Failures are not recorded: the operation stays unknown.) */
    public void ok(int handle, KVResult result) {
        Pending p = all.get(handle);
        p.ret = clock.getAsLong();
        p.result = result;
    }

    public List<Operation> operations() {
        List<Operation> out = new ArrayList<>(all.size());
        for (Pending p : all) {
            out.add(new Operation(p.id, p.client, p.kind, p.key, p.value, p.expected, p.invoke, p.ret, p.result));
        }
        return out;
    }

    public int size() {
        return all.size();
    }
}
