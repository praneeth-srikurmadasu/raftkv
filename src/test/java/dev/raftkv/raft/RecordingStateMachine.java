package dev.raftkv.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;

/**
 * A test state machine that records every command it applies and checks
 * <b>State Machine Safety</b> (Figure 3) on the spot: "if a server has applied a log entry at a
 * given index to its state machine, no other server will ever apply a different log entry for
 * the same index." All replicas share one {@code globallyApplied} map for that.
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
}
