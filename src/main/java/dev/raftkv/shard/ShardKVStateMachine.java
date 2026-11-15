package dev.raftkv.shard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Hashing;
import dev.raftkv.kv.KVCodec;
import dev.raftkv.kv.KVCommand;
import dev.raftkv.kv.KVResult;
import dev.raftkv.kv.SessionCommand;
import dev.raftkv.kv.SessionTable;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.raft.Bug;
import dev.raftkv.raft.StateMachine;

/**
 * The replicated state of one data group in the sharded cluster (SPEC §4.3, §6.4–6.6): for each
 * shard a status and, when the group holds it, its data and session table; plus the current and
 * previous configuration.
 *
 * <p>Two kinds of entries reach it:
 * <ul>
 *   <li><b>Client commands</b> ({@link SessionCommand}s). Ownership is checked <em>again at apply
 *       time</em>: the key's shard must be SERVING or GCING in the configuration this group has
 *       applied by then, otherwise the answer is WRONG_GROUP (or SHARD_NOT_READY while PULLING).
 *       Because configuration changes are log entries too, every replica switches at the same
 *       point in the operation order, and no operation is applied by both the old and the new
 *       owner.</li>
 *   <li><b>{@link ShardOp}s</b> the group writes for itself: ConfigChange, InsertShards,
 *       DeleteShards, GcDone. Each names its configuration and is ignored unless it matches the
 *       current state, so duplicates and late arrivals are harmless.</li>
 * </ul>
 * All migration state lives here, in the replicated state, never only in a leader's memory: a new
 * leader re-derives what remains to be done from the shard statuses.
 */
public final class ShardKVStateMachine implements StateMachine {

    /** Notified of every client command that really executes: (client, sequence, log index). */
    @FunctionalInterface
    public interface ExecutionObserver {
        void executed(UUID clientId, long sequence, long index);
    }

    /** Notified whenever an applied ConfigChange moves the group from one configuration to another. */
    @FunctionalInterface
    public interface ConfigObserver {
        void changed(ShardConfig from, ShardConfig to);
    }

    private static final byte SNAPSHOT_VERSION = 1;
    private static final long EXPIRY_SWEEP_MILLIS = 1_000;

    private final int gid;
    private final int numShards;
    private final long sessionTimeoutMillis;
    private final Set<Bug> bugs;
    private ShardConfig current;
    private ShardConfig prev;
    private final ShardStatus[] status;
    private final ShardData[] shards;
    private long logTimeMillis;
    private long nextSweepMillis;
    private long executed;
    private ExecutionObserver observer = (c, s, i) -> { };
    private ConfigObserver configObserver = (from, to) -> { };

    public ShardKVStateMachine(int gid, int numShards) {
        this(gid, numShards, Duration.ofHours(1), Set.of());
    }

    public ShardKVStateMachine(int gid, int numShards, Duration sessionTimeout, Set<Bug> bugs) {
        if (gid < 1) {
            throw new IllegalArgumentException("data group ids start at 1");
        }
        this.gid = gid;
        this.numShards = numShards;
        this.sessionTimeoutMillis = sessionTimeout.toMillis();
        this.bugs = Set.copyOf(bugs);
        this.current = ShardConfig.initial(numShards);
        this.prev = current;
        this.status = new ShardStatus[numShards];
        this.shards = new ShardData[numShards];
        java.util.Arrays.fill(status, ShardStatus.NOT_OWNED);
    }

    @Override
    public Bytes apply(long index, Bytes payload) {
        if (payload.isEmpty()) {
            return outcome(ClientStatus.INVALID_REQUEST, Bytes.EMPTY);
        }
        if (payload.get(0) == ShardOp.CLIENT_TAG) {
            return applyClient(index, payload);
        }
        ShardOp op;
        try {
            op = ShardOp.decode(payload);
        } catch (CodecException | IllegalArgumentException e) {
            return outcome(ClientStatus.INVALID_REQUEST, Bytes.EMPTY);
        }
        switch (op) {
            case ShardOp.ConfigChange c -> applyConfig(c.config());
            case ShardOp.InsertShards i -> applyInsert(i);
            case ShardOp.DeleteShards d -> applyDelete(d);
            case ShardOp.GcDone g -> applyGcDone(g);
        }
        return outcome(ClientStatus.OK, Bytes.EMPTY);
    }

    // ------------------------------------------------------------------ client commands

    private Bytes applyClient(long index, Bytes payload) {
        SessionCommand sc;
        KVCommand command;
        try {
            sc = KVCodec.decodeSessionCommand(payload);
            command = KVCodec.decodeCommand(sc.command());
        } catch (CodecException e) {
            return outcome(ClientStatus.INVALID_REQUEST, Bytes.EMPTY);
        }
        logTimeMillis = Math.max(logTimeMillis, sc.timestampMillis());
        if (logTimeMillis >= nextSweepMillis) {
            for (ShardData d : shards) {
                if (d != null) {
                    d.sessions.expireIdleBefore(logTimeMillis - sessionTimeoutMillis);
                }
            }
            nextSweepMillis = logTimeMillis + EXPIRY_SWEEP_MILLIS;
        }
        int shard = Shards.shardOf(command.key(), numShards);
        ShardStatus st = status[shard];
        boolean serves = st.serves()
                || (st == ShardStatus.BEPULLING && bugs.contains(Bug.SERVE_WHILE_BEPULLING))
                || bugs.contains(Bug.CHECK_OWNERSHIP_ON_RECEIVE_ONLY); // the server checked when it arrived
        if (!serves) {
            return outcome(st == ShardStatus.PULLING ? ClientStatus.SHARD_NOT_READY : ClientStatus.WRONG_GROUP, Bytes.EMPTY);
        }
        ShardData d = shards[shard];
        if (d == null) {
            d = new ShardData(); // only reachable with a bug switched on: a shard we hold nothing for
            shards[shard] = d;
        }
        SessionTable.Admission a = d.sessions.admit(sc.clientId(), sc.sequence(), sc.ackedBelow(), logTimeMillis,
                false, !bugs.contains(Bug.SESSION_DEDUP_DISABLED));
        switch (a.verdict()) {
            case EXPIRED -> {
                return outcome(ClientStatus.SESSION_EXPIRED, Bytes.EMPTY);
            }
            case CACHED -> {
                return a.cached();
            }
            case EXECUTE -> {
                // below
            }
        }
        Bytes result = outcome(ClientStatus.OK, KVCodec.encode(execute(d.data, command)));
        executed++;
        observer.executed(sc.clientId(), sc.sequence(), index);
        d.sessions.recordResult(sc.clientId(), sc.sequence(), result);
        return result;
    }

    private static KVResult execute(TreeMap<Bytes, Bytes> data, KVCommand command) {
        return switch (command) {
            case KVCommand.Put p -> new KVResult.PutResult(Optional.ofNullable(data.put(p.key(), p.value())));
            case KVCommand.Get g -> new KVResult.GetResult(Optional.ofNullable(data.get(g.key())));
            case KVCommand.Delete del -> new KVResult.DeleteResult(data.remove(del.key()) != null);
            case KVCommand.Cas c -> {
                Optional<Bytes> cur = Optional.ofNullable(data.get(c.key()));
                if (cur.equals(c.expected())) {
                    data.put(c.key(), c.newValue());
                    yield new KVResult.CasResult(true, Optional.of(c.newValue()));
                }
                yield new KVResult.CasResult(false, cur);
            }
        };
    }

    // ------------------------------------------------------------------ shard ops

    /**
     * SPEC §6.5: only the next configuration, and only once the previous migration has finished.
     * Shards gained become PULLING (SERVING if nobody owned them before), shards lost become
     * BEPULLING (frozen until the new owner confirms it has them), the rest keep serving.
     */
    private void applyConfig(ShardConfig next) {
        boolean inOrder = bugs.contains(Bug.SKIP_CONFIG_NUMBERS) ? next.num() > current.num() : next.num() == current.num() + 1;
        if (!inOrder || !allSettled() || next.numShards() != numShards) {
            return;
        }
        configObserver.changed(current, next);
        prev = current;
        current = next;
        for (int s = 0; s < numShards; s++) {
            boolean wasOurs = prev.gidOf(s) == gid;
            boolean isOurs = current.gidOf(s) == gid;
            if (!wasOurs && isOurs) {
                if (prev.gidOf(s) == 0) {
                    status[s] = ShardStatus.SERVING;
                    shards[s] = new ShardData();
                } else {
                    status[s] = ShardStatus.PULLING;
                }
            } else if (wasOurs && !isOurs) {
                status[s] = ShardStatus.BEPULLING;
            }
        }
    }

    private void applyInsert(ShardOp.InsertShards op) {
        if (op.configNum() != current.num()) {
            return;
        }
        for (Map.Entry<Integer, ShardData> e : op.shards().entrySet()) {
            int s = e.getKey();
            if (s >= 0 && s < numShards && status[s] == ShardStatus.PULLING) {
                ShardData d = e.getValue().copy();
                if (bugs.contains(Bug.SESSIONS_NOT_MIGRATED)) {
                    d.sessions = new SessionTable();
                }
                shards[s] = d;
                status[s] = ShardStatus.GCING;
            }
        }
    }

    private void applyDelete(ShardOp.DeleteShards op) {
        if (op.configNum() != current.num()) {
            return;
        }
        for (int s : op.shards()) {
            if (s >= 0 && s < numShards && status[s] == ShardStatus.BEPULLING) {
                shards[s] = null;
                status[s] = ShardStatus.NOT_OWNED;
            }
        }
    }

    private void applyGcDone(ShardOp.GcDone op) {
        if (op.configNum() != current.num()) {
            return;
        }
        for (int s : op.shards()) {
            if (s >= 0 && s < numShards && status[s] == ShardStatus.GCING) {
                status[s] = ShardStatus.SERVING;
            }
        }
    }

    private static Bytes outcome(ClientStatus status, Bytes result) {
        return KVCodec.encode(new KVCodec.Outcome(status, result));
    }

    // ------------------------------------------------------------------ queries for the server (event loop)

    /**
     * The receive-time ownership check the server does before proposing (saves a log entry for a
     * request that would be refused anyway): OK, WRONG_GROUP or SHARD_NOT_READY. The decisive check
     * is the one at apply time.
     */
    public ClientStatus receiveCheck(Bytes key) {
        ShardStatus st = status[Shards.shardOf(key, numShards)];
        if (st.serves() || (st == ShardStatus.BEPULLING && bugs.contains(Bug.SERVE_WHILE_BEPULLING))) {
            return ClientStatus.OK;
        }
        return st == ShardStatus.PULLING ? ClientStatus.SHARD_NOT_READY : ClientStatus.WRONG_GROUP;
    }

    /** Every shard is SERVING or NOT_OWNED: the previous configuration change is complete. */
    public boolean allSettled() {
        for (ShardStatus s : status) {
            if (s != ShardStatus.SERVING && s != ShardStatus.NOT_OWNED) {
                return false;
            }
        }
        return true;
    }

    public ShardConfig current() {
        return current;
    }

    public ShardConfig previous() {
        return prev;
    }

    public int gid() {
        return gid;
    }

    public int numShards() {
        return numShards;
    }

    public ShardStatus status(int shard) {
        return status[shard];
    }

    /** Shards currently in {@code s}, ascending. */
    public List<Integer> shardsIn(ShardStatus s) {
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            if (status[i] == s) {
                out.add(i);
            }
        }
        return out;
    }

    /**
     * The frozen copies of {@code requested} for a FetchShard of configuration {@code configNum}, or
     * empty if this replica can't answer yet. Any replica that has applied that configuration can
     * answer: a BEPULLING shard never changes, so all of them hold the same bytes.
     */
    public Optional<SortedMap<Integer, ShardData>> frozenShards(long configNum, List<Integer> requested) {
        if (current.num() != configNum) {
            return Optional.empty();
        }
        SortedMap<Integer, ShardData> out = new TreeMap<>();
        for (int s : requested) {
            if (s < 0 || s >= numShards || status[s] != ShardStatus.BEPULLING) {
                return Optional.empty();
            }
            out.put(s, shards[s].copy());
        }
        return Optional.of(out);
    }

    /** True if this group no longer holds {@code requested} for {@code configNum} (deleted, or already past it). */
    public boolean deleted(long configNum, List<Integer> requested) {
        if (current.num() > configNum) {
            return true;
        }
        if (current.num() < configNum) {
            return false;
        }
        for (int s : requested) {
            if (s >= 0 && s < numShards && status[s] == ShardStatus.BEPULLING) {
                return false;
            }
        }
        return true;
    }

    /** The contents of a shard this group holds (tests); empty if it holds none. */
    public Optional<ShardData> shardData(int shard) {
        return Optional.ofNullable(shards[shard]);
    }

    /** Current value of {@code key} in this replica (tests; not linearizable on its own). */
    public Optional<Bytes> peek(Bytes key) {
        ShardData d = shards[Shards.shardOf(key, numShards)];
        return d == null ? Optional.empty() : Optional.ofNullable(d.data.get(key));
    }

    public long executedCommands() {
        return executed;
    }

    public void setExecutionObserver(ExecutionObserver o) {
        this.observer = Objects.requireNonNull(o, "observer");
    }

    public void setConfigObserver(ConfigObserver o) {
        this.configObserver = Objects.requireNonNull(o, "observer");
    }

    public long digest() {
        return Hashing.fnv1a64(snapshot().toArray());
    }

    // ------------------------------------------------------------------ snapshots

    @Override
    public Bytes snapshot() {
        BinaryWriter w = new BinaryWriter();
        w.writeByte(SNAPSHOT_VERSION);
        w.writeInt(gid);
        w.writeLong(logTimeMillis);
        w.writeLong(nextSweepMillis);
        w.writeLong(executed);
        current.writeTo(w);
        prev.writeTo(w);
        w.writeInt(numShards);
        for (int s = 0; s < numShards; s++) {
            w.writeByte(status[s].code());
            w.writeBoolean(shards[s] != null);
            if (shards[s] != null) {
                shards[s].writeTo(w);
            }
        }
        return w.toBytes();
    }

    @Override
    public void restore(Bytes snapshot) {
        BinaryReader r = new BinaryReader(snapshot);
        if (r.readByte() != SNAPSHOT_VERSION) {
            throw new CodecException("unknown shard snapshot version");
        }
        if (r.readInt() != gid) {
            throw new CodecException("snapshot belongs to another group");
        }
        long time = r.readLong();
        long sweep = r.readLong();
        long exec = r.readLong();
        ShardConfig cur = ShardConfig.readFrom(r);
        ShardConfig pre = ShardConfig.readFrom(r);
        if (r.readInt() != numShards) {
            throw new CodecException("snapshot has a different shard count");
        }
        ShardStatus[] st = new ShardStatus[numShards];
        ShardData[] data = new ShardData[numShards];
        for (int s = 0; s < numShards; s++) {
            st[s] = ShardStatus.fromCode(r.readByte());
            if (r.readBoolean()) {
                data[s] = ShardData.readFrom(r);
            }
        }
        r.expectEnd();
        logTimeMillis = time;
        nextSweepMillis = sweep;
        executed = exec;
        current = cur;
        prev = pre;
        System.arraycopy(st, 0, status, 0, numShards);
        System.arraycopy(data, 0, shards, 0, numShards);
    }
}
