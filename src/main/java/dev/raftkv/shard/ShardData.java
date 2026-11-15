package dev.raftkv.shard;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.common.Bytes;
import dev.raftkv.kv.SessionTable;

/**
 * The contents of one shard (SPEC §4.3): its keys and values, and its session table. Sessions
 * live per shard so that the dedup state for a key always travels with the key when the shard
 * moves to another group: a client retrying a write at the new owner gets the cached result.
 */
public final class ShardData {

    final TreeMap<Bytes, Bytes> data;
    SessionTable sessions;

    public ShardData() {
        this(new TreeMap<>(), new SessionTable());
    }

    ShardData(TreeMap<Bytes, Bytes> data, SessionTable sessions) {
        this.data = Objects.requireNonNull(data, "data");
        this.sessions = Objects.requireNonNull(sessions, "sessions");
    }

    /** A deep copy (what a FetchShard reply carries must not share state with the frozen original). */
    public ShardData copy() {
        return new ShardData(new TreeMap<>(data), sessions.copy());
    }

    public int size() {
        return data.size();
    }

    public SessionTable sessions() {
        return sessions;
    }

    /** Read-only view of the keys and values (tests). */
    public Map<Bytes, Bytes> entries() {
        return java.util.Collections.unmodifiableMap(data);
    }

    public void writeTo(BinaryWriter w) {
        w.writeInt(data.size());
        for (Map.Entry<Bytes, Bytes> e : data.entrySet()) {
            w.writeBytes(e.getKey());
            w.writeBytes(e.getValue());
        }
        sessions.writeTo(w);
    }

    public static ShardData readFrom(BinaryReader r) {
        int n = SessionTable.readCount(r, 8);
        TreeMap<Bytes, Bytes> data = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            data.put(r.readBytes(), r.readBytes());
        }
        return new ShardData(data, SessionTable.readFrom(r));
    }
}
