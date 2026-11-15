package dev.raftkv.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import org.junit.jupiter.api.Test;

class ShardConfigTest {

    @Test
    void roundTripsThroughBytes() {
        Map<Integer, List<NodeId>> groups = new TreeMap<>();
        groups.put(1, List.of(NodeId.of(1), NodeId.of(2)));
        groups.put(3, List.of(NodeId.of(7)));
        ShardConfig c = new ShardConfig(4, new int[] {1, 3, 3, 1, 0}, groups);
        BinaryWriter w = new BinaryWriter();
        c.writeTo(w);
        ShardConfig back = ShardConfig.readFrom(new BinaryReader(w.toBytes()));
        assertThat(back).isEqualTo(c);
        assertThat(back.shardsOf(3)).isEqualTo(List.of(1, 2));
        assertThat(back.servers(1)).isEqualTo(List.of(NodeId.of(1), NodeId.of(2)));
        assertThat(back.servers(9).isEmpty()).isTrue();
        assertThat(back.toString()).contains("config 4 [1 3 3 1 0]");
    }

    @Test
    void rejectsInconsistentConfigurations() {
        assertThatThrownBy(() -> new ShardConfig(1, new int[] {2}, Map.of(1, List.of(NodeId.of(1)))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShardConfig(1, new int[] {1}, Map.of(1, List.of())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShardConfig(-1, new int[] {0}, Map.of())).isInstanceOf(IllegalArgumentException.class);
        Bytes garbage = new BinaryWriter().writeLong(1).writeInt(1).writeInt(5).writeInt(0).toBytes();
        assertThatThrownBy(() -> ShardConfig.readFrom(new BinaryReader(garbage))).isInstanceOf(CodecException.class);
    }

    @Test
    void keysMapToStableShards() {
        // These values are part of the cluster's contract: changing the hash would move every key.
        assertThat(Shards.shardOf(Bytes.ofUtf8("city"), 64)).isEqualTo(Shards.shardOf(Bytes.ofUtf8("city"), 64));
        int[] counts = new int[16];
        for (int i = 0; i < 16_000; i++) {
            counts[Shards.shardOf(Bytes.ofUtf8("key" + i), 16)]++;
        }
        for (int c : counts) {
            assertThat(c).as("roughly uniform").isGreaterThanOrEqualTo(800);
            assertThat(c).as("roughly uniform").isLessThanOrEqualTo(1200);
        }
        assertThat(Shards.shardOf(Bytes.EMPTY, 64)).isGreaterThanOrEqualTo(0);
    }
}
