package dev.raftkv.shard;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.kv.SessionTable;

/**
 * The log entries a data group writes for itself while shards move (SPEC §6.5, §6.6), next to the
 * client commands. Each is tagged with the configuration it belongs to and ignored if the group's
 * state no longer matches it, which makes every one of them safe to apply twice.
 */
public sealed interface ShardOp {

    /** First byte of a client command entry (the KV store's {@code SessionCommand} payload). */
    byte CLIENT_TAG = 1;
    byte CONFIG_CHANGE = 10;
    byte INSERT_SHARDS = 11;
    byte DELETE_SHARDS = 12;
    byte GC_DONE = 13;

    /** Switch to the next configuration. */
    record ConfigChange(ShardConfig config) implements ShardOp {
    }

    /** Install shards fetched from their previous owner (PULLING → GCING). */
    record InsertShards(long configNum, SortedMap<Integer, ShardData> shards) implements ShardOp {
    }

    /** Drop frozen shards the new owner has installed (BEPULLING → NOT_OWNED). */
    record DeleteShards(long configNum, List<Integer> shards) implements ShardOp {
        public DeleteShards {
            shards = List.copyOf(shards);
        }
    }

    /** The previous owner confirmed the delete (GCING → SERVING). */
    record GcDone(long configNum, List<Integer> shards) implements ShardOp {
        public GcDone {
            shards = List.copyOf(shards);
        }
    }

    static Bytes encode(ShardOp op) {
        BinaryWriter w = new BinaryWriter();
        switch (op) {
            case ConfigChange c -> {
                w.writeByte(CONFIG_CHANGE);
                c.config().writeTo(w);
            }
            case InsertShards i -> {
                w.writeByte(INSERT_SHARDS);
                w.writeLong(i.configNum());
                writeShardMap(w, i.shards());
            }
            case DeleteShards d -> {
                w.writeByte(DELETE_SHARDS);
                w.writeLong(d.configNum());
                writeList(w, d.shards());
            }
            case GcDone g -> {
                w.writeByte(GC_DONE);
                w.writeLong(g.configNum());
                writeList(w, g.shards());
            }
        }
        return w.toBytes();
    }

    static ShardOp decode(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        byte tag = r.readByte();
        ShardOp op = switch (tag) {
            case CONFIG_CHANGE -> new ConfigChange(ShardConfig.readFrom(r));
            case INSERT_SHARDS -> new InsertShards(r.readLong(), readShardMap(r));
            case DELETE_SHARDS -> new DeleteShards(r.readLong(), readList(r));
            case GC_DONE -> new GcDone(r.readLong(), readList(r));
            default -> throw new CodecException("unknown shard op tag " + tag);
        };
        r.expectEnd();
        return op;
    }

    /** The payload of a FetchShard reply: shard number → contents. */
    static Bytes encodeShards(SortedMap<Integer, ShardData> shards) {
        BinaryWriter w = new BinaryWriter();
        writeShardMap(w, shards);
        return w.toBytes();
    }

    static SortedMap<Integer, ShardData> decodeShards(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        SortedMap<Integer, ShardData> m = readShardMap(r);
        r.expectEnd();
        return m;
    }

    private static void writeShardMap(BinaryWriter w, SortedMap<Integer, ShardData> shards) {
        w.writeInt(shards.size());
        for (Map.Entry<Integer, ShardData> e : shards.entrySet()) {
            w.writeInt(e.getKey());
            e.getValue().writeTo(w);
        }
    }

    private static SortedMap<Integer, ShardData> readShardMap(BinaryReader r) {
        int n = SessionTable.readCount(r, 12);
        SortedMap<Integer, ShardData> m = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            m.put(r.readInt(), ShardData.readFrom(r));
        }
        return m;
    }

    private static void writeList(BinaryWriter w, List<Integer> shards) {
        w.writeInt(shards.size());
        for (int s : shards) {
            w.writeInt(s);
        }
    }

    private static List<Integer> readList(BinaryReader r) {
        int n = SessionTable.readCount(r, 4);
        Integer[] out = new Integer[n];
        for (int i = 0; i < n; i++) {
            out[i] = r.readInt();
        }
        return List.of(out);
    }
}
