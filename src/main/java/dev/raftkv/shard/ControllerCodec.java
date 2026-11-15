package dev.raftkv.shard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;

/** Binary encoding of {@link ControllerCommand}s and of configurations as results. */
public final class ControllerCodec {

    private static final byte JOIN = 1;
    private static final byte LEAVE = 2;
    private static final byte MOVE = 3;
    private static final byte QUERY = 4;

    private ControllerCodec() {
    }

    public static Bytes encode(ControllerCommand command) {
        BinaryWriter w = new BinaryWriter();
        switch (command) {
            case ControllerCommand.Join j -> {
                w.writeByte(JOIN);
                w.writeInt(j.groups().size());
                for (Map.Entry<Integer, List<NodeId>> e : j.groups().entrySet()) {
                    w.writeInt(e.getKey());
                    w.writeInt(e.getValue().size());
                    for (NodeId n : e.getValue()) {
                        w.writeInt(n.value());
                    }
                }
            }
            case ControllerCommand.Leave l -> {
                w.writeByte(LEAVE);
                w.writeInt(l.gids().size());
                for (int gid : l.gids()) {
                    w.writeInt(gid);
                }
            }
            case ControllerCommand.Move m -> {
                w.writeByte(MOVE);
                w.writeInt(m.shard());
                w.writeInt(m.gid());
            }
            case ControllerCommand.Query q -> {
                w.writeByte(QUERY);
                w.writeLong(q.num());
            }
        }
        return w.toBytes();
    }

    /** Decodes a command; throws {@link CodecException} for anything malformed. */
    public static ControllerCommand decode(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        ControllerCommand c;
        try {
            byte tag = r.readByte();
            c = switch (tag) {
                case JOIN -> {
                    int n = count(r, 8);
                    SortedMap<Integer, List<NodeId>> groups = new TreeMap<>();
                    for (int i = 0; i < n; i++) {
                        int gid = r.readInt();
                        int s = count(r, 4);
                        List<NodeId> servers = new ArrayList<>(s);
                        for (int j = 0; j < s; j++) {
                            servers.add(new NodeId(r.readInt()));
                        }
                        groups.put(gid, servers);
                    }
                    yield new ControllerCommand.Join(groups);
                }
                case LEAVE -> {
                    int n = count(r, 4);
                    List<Integer> gids = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        gids.add(r.readInt());
                    }
                    yield new ControllerCommand.Leave(gids);
                }
                case MOVE -> new ControllerCommand.Move(r.readInt(), r.readInt());
                case QUERY -> new ControllerCommand.Query(r.readLong());
                default -> throw new CodecException("unknown controller command tag " + tag);
            };
        } catch (IllegalArgumentException e) {
            throw new CodecException("invalid controller command: " + e.getMessage());
        }
        r.expectEnd();
        return c;
    }

    public static Bytes encode(ShardConfig config) {
        BinaryWriter w = new BinaryWriter();
        config.writeTo(w);
        return w.toBytes();
    }

    public static ShardConfig decodeConfig(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        ShardConfig c = ShardConfig.readFrom(r);
        r.expectEnd();
        return c;
    }

    private static int count(BinaryReader r, int minBytes) {
        int c = r.readInt();
        if (c < 0 || (long) c * minBytes > r.remaining()) {
            throw new CodecException("invalid count " + c);
        }
        return c;
    }
}
