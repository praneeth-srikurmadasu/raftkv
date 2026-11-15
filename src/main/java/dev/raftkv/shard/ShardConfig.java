package dev.raftkv.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.NodeId;

/**
 * One numbered configuration of the sharded cluster (SPEC §6.3): which replica group owns each
 * shard, and which servers make up each group. Immutable.
 *
 * <p>Group id 0 means "unassigned"; configuration 0 has no groups and every shard on gid 0. Data
 * groups have ids from 1 (group 0 is the controller's own Raft group).
 */
public final class ShardConfig {

    private final long num;
    private final int[] shardToGid;
    private final SortedMap<Integer, List<NodeId>> groups;

    public ShardConfig(long num, int[] shardToGid, Map<Integer, List<NodeId>> groups) {
        if (num < 0) {
            throw new IllegalArgumentException("config num must be >= 0, was " + num);
        }
        if (shardToGid.length < 1) {
            throw new IllegalArgumentException("need at least one shard");
        }
        TreeMap<Integer, List<NodeId>> g = new TreeMap<>();
        for (Map.Entry<Integer, List<NodeId>> e : groups.entrySet()) {
            if (e.getKey() < 1) {
                throw new IllegalArgumentException("data group ids start at 1, got " + e.getKey());
            }
            if (e.getValue().isEmpty()) {
                throw new IllegalArgumentException("group " + e.getKey() + " has no servers");
            }
            g.put(e.getKey(), List.copyOf(e.getValue()));
        }
        for (int s = 0; s < shardToGid.length; s++) {
            int gid = shardToGid[s];
            if (gid != 0 && !g.containsKey(gid)) {
                throw new IllegalArgumentException("shard " + s + " is assigned to unknown group " + gid);
            }
        }
        this.num = num;
        this.shardToGid = shardToGid.clone();
        this.groups = Collections.unmodifiableSortedMap(g);
    }

    /** Configuration 0: no groups, every shard unassigned. */
    public static ShardConfig initial(int numShards) {
        return new ShardConfig(0, new int[numShards], Map.of());
    }

    public long num() {
        return num;
    }

    public int numShards() {
        return shardToGid.length;
    }

    /** The group that owns {@code shard} (0 = unassigned). */
    public int gidOf(int shard) {
        return shardToGid[shard];
    }

    /** A copy of the shard -> group assignment. */
    public int[] assignment() {
        return shardToGid.clone();
    }

    public SortedMap<Integer, List<NodeId>> groups() {
        return groups;
    }

    public SortedSet<Integer> gids() {
        return new TreeSet<>(groups.keySet());
    }

    /** The servers of {@code gid} (empty if the group is not in this configuration). */
    public List<NodeId> servers(int gid) {
        return groups.getOrDefault(gid, List.of());
    }

    /** The shards owned by {@code gid}, ascending. */
    public List<Integer> shardsOf(int gid) {
        List<Integer> out = new ArrayList<>();
        for (int s = 0; s < shardToGid.length; s++) {
            if (shardToGid[s] == gid) {
                out.add(s);
            }
        }
        return out;
    }

    /** The next configuration: number + 1, with the given assignment and groups. */
    public ShardConfig next(int[] newAssignment, Map<Integer, List<NodeId>> newGroups) {
        return new ShardConfig(num + 1, newAssignment, newGroups);
    }

    // ------------------------------------------------------------------ encoding

    public void writeTo(BinaryWriter w) {
        w.writeLong(num);
        w.writeInt(shardToGid.length);
        for (int gid : shardToGid) {
            w.writeInt(gid);
        }
        w.writeInt(groups.size());
        for (Map.Entry<Integer, List<NodeId>> e : groups.entrySet()) {
            w.writeInt(e.getKey());
            w.writeInt(e.getValue().size());
            for (NodeId n : e.getValue()) {
                w.writeInt(n.value());
            }
        }
    }

    public static ShardConfig readFrom(BinaryReader r) {
        try {
            long num = r.readLong();
            int n = count(r, 4);
            int[] assignment = new int[n];
            for (int i = 0; i < n; i++) {
                assignment[i] = r.readInt();
            }
            int g = count(r, 8);
            Map<Integer, List<NodeId>> groups = new TreeMap<>();
            for (int i = 0; i < g; i++) {
                int gid = r.readInt();
                int servers = count(r, 4);
                List<NodeId> list = new ArrayList<>(servers);
                for (int j = 0; j < servers; j++) {
                    list.add(new NodeId(r.readInt()));
                }
                groups.put(gid, list);
            }
            return new ShardConfig(num, assignment, groups);
        } catch (IllegalArgumentException e) {
            throw new CodecException("invalid shard config: " + e.getMessage());
        }
    }

    private static int count(BinaryReader r, int minBytes) {
        int c = r.readInt();
        if (c < 0 || (long) c * minBytes > r.remaining()) {
            throw new CodecException("invalid count " + c);
        }
        return c;
    }

    // ------------------------------------------------------------------ value semantics

    @Override
    public boolean equals(Object o) {
        return o instanceof ShardConfig other && num == other.num && Arrays.equals(shardToGid, other.shardToGid)
                && groups.equals(other.groups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(num, Arrays.hashCode(shardToGid), groups);
    }

    /** E.g. {@code config 3 [1 1 2 2 3 3] groups {1=[n1, n2, n3], ...}}. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("config ").append(num).append(" [");
        for (int s = 0; s < shardToGid.length; s++) {
            sb.append(s == 0 ? "" : " ").append(shardToGid[s]);
        }
        return sb.append("] groups ").append(groups).toString();
    }
}
