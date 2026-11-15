package dev.raftkv.shard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Hashing;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVCodec;
import dev.raftkv.kv.SessionCommand;
import dev.raftkv.kv.SessionTable;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.raft.StateMachine;

/**
 * The shard controller's replicated state (SPEC §6.3): the list of every configuration ever made.
 * Runs as the state machine of Raft group 0. Entries are {@link SessionCommand}s wrapping
 * {@link ControllerCommand}s, with the same exactly-once sessions and log-time expiry as the KV store.
 *
 * <p>Rules that protect data:
 * <ul>
 *   <li>A Leave that would remove every group is refused: shards would go back to gid 0, and a
 *       group that later gets a shard from gid 0 starts it empty.</li>
 *   <li>Move only targets an existing group.</li>
 * </ul>
 * Every command's result is a configuration: the new one for Join/Leave/Move, the requested one for Query.
 */
public final class ControllerStateMachine implements StateMachine {

    private static final byte SNAPSHOT_VERSION = 1;
    private static final long EXPIRY_SWEEP_MILLIS = 1_000;

    private final int numShards;
    private final long sessionTimeoutMillis;
    private final List<ShardConfig> configs = new ArrayList<>();
    private SessionTable sessions = new SessionTable();
    private long logTimeMillis;
    private long nextSweepMillis;

    public ControllerStateMachine(int numShards) {
        this(numShards, Duration.ofHours(1));
    }

    public ControllerStateMachine(int numShards, Duration sessionTimeout) {
        if (numShards < 1) {
            throw new IllegalArgumentException("need at least one shard");
        }
        this.numShards = numShards;
        this.sessionTimeoutMillis = sessionTimeout.toMillis();
        configs.add(ShardConfig.initial(numShards));
    }

    @Override
    public Bytes apply(long index, Bytes payload) {
        SessionCommand sc;
        try {
            sc = KVCodec.decodeSessionCommand(payload);
        } catch (CodecException e) {
            return outcome(ClientStatus.INVALID_REQUEST, Bytes.EMPTY);
        }
        logTimeMillis = Math.max(logTimeMillis, sc.timestampMillis());
        if (logTimeMillis >= nextSweepMillis) {
            sessions.expireIdleBefore(logTimeMillis - sessionTimeoutMillis);
            nextSweepMillis = logTimeMillis + EXPIRY_SWEEP_MILLIS;
        }
        SessionTable.Admission a = sessions.admit(sc.clientId(), sc.sequence(), sc.ackedBelow(), logTimeMillis, true, true);
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
        ShardConfig result;
        try {
            result = execute(ControllerCodec.decode(sc.command()));
        } catch (CodecException | IllegalArgumentException e) {
            return outcome(ClientStatus.INVALID_REQUEST, Bytes.EMPTY);
        }
        Bytes out = outcome(ClientStatus.OK, ControllerCodec.encode(result));
        sessions.recordResult(sc.clientId(), sc.sequence(), out);
        return out;
    }

    private ShardConfig execute(ControllerCommand command) {
        ShardConfig latest = latest();
        return switch (command) {
            case ControllerCommand.Query q -> q.num() < 0 || q.num() >= configs.size() ? latest : configs.get((int) q.num());
            case ControllerCommand.Join j -> {
                SortedMap<Integer, List<NodeId>> groups = new TreeMap<>(latest.groups());
                groups.putAll(j.groups());
                yield add(latest.next(Rebalancer.rebalance(latest.assignment(), new TreeSet<>(groups.keySet())), groups));
            }
            case ControllerCommand.Leave l -> {
                SortedMap<Integer, List<NodeId>> groups = new TreeMap<>(latest.groups());
                for (int gid : l.gids()) {
                    if (groups.remove(gid) == null) {
                        throw new IllegalArgumentException("group " + gid + " is not in the configuration");
                    }
                }
                if (groups.isEmpty()) {
                    throw new IllegalArgumentException("cannot remove every group");
                }
                yield add(latest.next(Rebalancer.rebalance(latest.assignment(), new TreeSet<>(groups.keySet())), groups));
            }
            case ControllerCommand.Move m -> {
                if (m.shard() >= numShards || !latest.groups().containsKey(m.gid())) {
                    throw new IllegalArgumentException("cannot move shard " + m.shard() + " to group " + m.gid());
                }
                int[] assignment = latest.assignment();
                assignment[m.shard()] = m.gid();
                yield add(latest.next(assignment, latest.groups()));
            }
        };
    }

    private ShardConfig add(ShardConfig c) {
        configs.add(c);
        return c;
    }

    private static Bytes outcome(ClientStatus status, Bytes result) {
        return KVCodec.encode(new KVCodec.Outcome(status, result));
    }

    public ShardConfig latest() {
        return configs.get(configs.size() - 1);
    }

    /** Configuration {@code num} (tests). */
    public ShardConfig config(long num) {
        return configs.get((int) num);
    }

    public int numShards() {
        return numShards;
    }

    /** Fingerprint of the replicated state (replicas must agree). */
    public long digest() {
        return Hashing.fnv1a64(snapshot().toArray());
    }

    // ------------------------------------------------------------------ snapshots

    @Override
    public Bytes snapshot() {
        BinaryWriter w = new BinaryWriter();
        w.writeByte(SNAPSHOT_VERSION);
        w.writeLong(logTimeMillis);
        w.writeLong(nextSweepMillis);
        sessions.writeTo(w);
        w.writeInt(configs.size());
        for (ShardConfig c : configs) {
            c.writeTo(w);
        }
        return w.toBytes();
    }

    @Override
    public void restore(Bytes snapshot) {
        BinaryReader r = new BinaryReader(snapshot);
        if (r.readByte() != SNAPSHOT_VERSION) {
            throw new CodecException("unknown controller snapshot version");
        }
        long time = r.readLong();
        long sweep = r.readLong();
        SessionTable table = SessionTable.readFrom(r);
        int n = SessionTable.readCount(r, 16);
        List<ShardConfig> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(ShardConfig.readFrom(r));
        }
        r.expectEnd();
        if (list.isEmpty() || list.get(0).numShards() != numShards) {
            throw new CodecException("controller snapshot is for a different shard count");
        }
        logTimeMillis = time;
        nextSweepMillis = sweep;
        sessions = table;
        configs.clear();
        configs.addAll(list);
    }

    /** All configurations (tests). */
    public List<ShardConfig> history() {
        return List.copyOf(configs);
    }

    static Map<Integer, Integer> countsByGroup(ShardConfig c) {
        Map<Integer, Integer> counts = new TreeMap<>();
        for (int s = 0; s < c.numShards(); s++) {
            counts.merge(c.gidOf(s), 1, Integer::sum);
        }
        return counts;
    }
}
