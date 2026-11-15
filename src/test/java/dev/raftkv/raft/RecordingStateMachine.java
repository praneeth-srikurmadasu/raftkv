package dev.raftkv.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;

/**
 * A test state machine that records every command it applies and checks
 * <b>State Machine Safety</b> (Figure 3) on the spot: "if a server has applied a log entry at a
 * given index to its state machine, no other server will ever apply a different log entry for
 * the same index." All replicas share one {@code globallyApplied} map for that.
 *
 * <p>Its snapshot holds the whole applied history (plus the delegate's snapshot), so a replica
 * restored from a snapshot still reports the full history and tests can compare replicas.
 */
final class RecordingStateMachine implements StateMachine {

    record Applied(long index, Bytes command) {}

    private final NodeId node;
    private final Map<Long, Bytes> globallyApplied;
    private final StateMachine delegate;
    private final List<Applied> applied = new ArrayList<>();
    private long lastIndex;

    RecordingStateMachine(NodeId node, Map<Long, Bytes> globallyApplied) {
        this(node, globallyApplied, null);
    }

    /** Records and checks, then lets {@code delegate} (e.g. the KV store) do the real work. */
    RecordingStateMachine(NodeId node, Map<Long, Bytes> globallyApplied, StateMachine delegate) {
        this.node = node;
        this.globallyApplied = globallyApplied;
        this.delegate = delegate;
    }

    StateMachine delegate() {
        return delegate;
    }

    @Override
    public Bytes apply(long index, Bytes command) {
        if (index <= lastIndex) {
            throw new AssertionError(node + " applied index " + index + " after " + lastIndex + " (out of order)");
        }
        lastIndex = index;
        Bytes earlier = globallyApplied.putIfAbsent(index, command);
        if (earlier != null && !earlier.equals(command)) {
            throw new AssertionError("State Machine Safety violated: index " + index + " applied as "
                    + earlier.toUtf8() + " elsewhere but as " + command.toUtf8() + " on " + node);
        }
        applied.add(new Applied(index, command));
        return delegate != null ? delegate.apply(index, command) : Bytes.ofUtf8("ok:" + command.toUtf8());
    }

    List<Applied> applied() {
        return applied;
    }

    long lastIndex() {
        return lastIndex;
    }

    @Override
    public Bytes snapshot() {
        BinaryWriter w = new BinaryWriter();
        w.writeLong(lastIndex);
        w.writeInt(applied.size());
        for (Applied a : applied) {
            w.writeLong(a.index());
            w.writeBytes(a.command());
        }
        w.writeBytes(delegate != null ? delegate.snapshot() : Bytes.EMPTY);
        return w.toBytes();
    }

    @Override
    public void restore(Bytes snapshot) {
        BinaryReader r = new BinaryReader(snapshot);
        long last = r.readLong();
        if (last < lastIndex) {
            throw new AssertionError(node + " restored a snapshot at " + last + " after applying " + lastIndex
                    + " (state went backwards)");
        }
        int n = r.readInt();
        List<Applied> history = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            history.add(new Applied(r.readLong(), r.readBytes()));
        }
        Bytes inner = r.readBytes();
        r.expectEnd();
        for (Applied a : history) {
            Bytes earlier = globallyApplied.putIfAbsent(a.index(), a.command());
            if (earlier != null && !earlier.equals(a.command())) {
                throw new AssertionError("State Machine Safety violated by a snapshot: index " + a.index() + " is "
                        + earlier.toUtf8() + " elsewhere but " + a.command().toUtf8() + " in the snapshot on " + node);
            }
        }
        lastIndex = last;
        applied.clear();
        applied.addAll(history);
        if (delegate != null) {
            delegate.restore(inner);
        }
    }
}
