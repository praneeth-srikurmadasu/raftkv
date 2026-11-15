package dev.raftkv.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.UUID;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVCodec;
import dev.raftkv.kv.KVCommand;
import dev.raftkv.kv.KVResult;
import dev.raftkv.kv.SessionCommand;
import dev.raftkv.protocol.ClientStatus;
import org.junit.jupiter.api.Test;

/** The shard status machine of SPEC §6.4-6.6, one entry at a time. */
class ShardKVStateMachineTest {

    private static final int N = 4;
    private static final UUID CLIENT = new UUID(1, 1);
    private static final Map<Integer, List<NodeId>> GROUPS = Map.of(1, List.of(NodeId.of(1)), 2, List.of(NodeId.of(2)));

    private final ShardKVStateMachine g1 = new ShardKVStateMachine(1, N);
    private final ShardKVStateMachine g2 = new ShardKVStateMachine(2, N);
    private long index;

    /** A key that lives in {@code shard}. */
    private static Bytes keyIn(int shard) {
        for (int i = 0; ; i++) {
            Bytes k = Bytes.ofUtf8("k" + i);
            if (Shards.shardOf(k, N) == shard) {
                return k;
            }
        }
    }

    private static ShardConfig config(long num, int... assignment) {
        return new ShardConfig(num, assignment, GROUPS);
    }

    private void apply(ShardKVStateMachine sm, ShardOp op) {
        sm.apply(++index, ShardOp.encode(op));
    }

    private KVCodec.Outcome client(ShardKVStateMachine sm, long seq, KVCommand cmd) {
        Bytes payload = KVCodec.encode(new SessionCommand(CLIENT, seq, 1, 1000, KVCodec.encode(cmd)));
        return KVCodec.decodeOutcome(sm.apply(++index, payload));
    }

    /** Both groups at config 2: shards 0,1 on g1 and 2,3 on g2 after config 1 gave g1 everything. */
    private void moveHalfToGroup2() {
        for (ShardKVStateMachine sm : List.of(g1, g2)) {
            apply(sm, new ShardOp.ConfigChange(config(1, 1, 1, 1, 1)));
        }
        client(g1, 1, new KVCommand.Put(keyIn(2), Bytes.ofUtf8("v2")));
        for (ShardKVStateMachine sm : List.of(g1, g2)) {
            apply(sm, new ShardOp.ConfigChange(config(2, 1, 1, 2, 2)));
        }
    }

    @Test
    void nothingIsOwnedBeforeTheFirstConfiguration() {
        assertThat(client(g1, 1, new KVCommand.Get(keyIn(0))).status()).isEqualTo(ClientStatus.WRONG_GROUP);
        assertThat(g1.allSettled()).isTrue();
    }

    @Test
    void shardsFromNobodyStartServingEmpty() {
        apply(g1, new ShardOp.ConfigChange(config(1, 1, 1, 1, 1)));
        for (int s = 0; s < N; s++) {
            assertThat(g1.status(s)).isEqualTo(ShardStatus.SERVING);
        }
        assertThat(client(g1, 1, new KVCommand.Put(keyIn(3), Bytes.ofUtf8("x"))).status()).isEqualTo(ClientStatus.OK);
    }

    @Test
    void theFullMigrationOfOneShard() {
        moveHalfToGroup2();
        // Old owner: frozen, refuses clients, can't move on until the shard is gone.
        assertThat(g1.status(2)).isEqualTo(ShardStatus.BEPULLING);
        assertThat(client(g1, 2, new KVCommand.Get(keyIn(2))).status()).isEqualTo(ClientStatus.WRONG_GROUP);
        assertThat(g1.allSettled()).isFalse();
        apply(g1, new ShardOp.ConfigChange(config(3, 1, 1, 1, 1)));
        assertThat(g1.current().num()).as("config 3 must wait").isEqualTo(2L);
        // New owner: waiting for the data.
        assertThat(g2.status(2)).isEqualTo(ShardStatus.PULLING);
        assertThat(client(g2, 2, new KVCommand.Get(keyIn(2))).status()).isEqualTo(ClientStatus.SHARD_NOT_READY);
        assertThat(g2.frozenShards(2, List.of(2)).isPresent()).as("only the old owner can answer").isFalse();

        // Fetch + insert.
        SortedMap<Integer, ShardData> fetched = g1.frozenShards(2, List.of(2, 3)).orElseThrow();
        assertThat(g1.frozenShards(1, List.of(2)).isPresent()).as("wrong config").isFalse();
        apply(g2, new ShardOp.InsertShards(1, fetched)); // stale config: ignored
        assertThat(g2.status(2)).isEqualTo(ShardStatus.PULLING);
        apply(g2, new ShardOp.InsertShards(2, fetched));
        assertThat(g2.status(2)).isEqualTo(ShardStatus.GCING);
        assertThat(client(g2, 3, new KVCommand.Get(keyIn(2))).status()).isEqualTo(ClientStatus.OK);

        // The write that ran at g1 travels with its session: a retry at g2 is answered from the cache.
        long executed = g2.executedCommands();
        KVCodec.Outcome retry = client(g2, 1, new KVCommand.Put(keyIn(2), Bytes.ofUtf8("v2")));
        assertThat(KVCodec.decodeResult(retry.result())).isEqualTo(new KVResult.PutResult(Optional.empty()));
        assertThat(g2.executedCommands()).isEqualTo(executed);

        // A second insert of the same shard is ignored (it is no longer PULLING).
        apply(g2, new ShardOp.InsertShards(2, g1.frozenShards(2, List.of(2, 3)).orElseThrow()));
        assertThat(g2.peek(keyIn(2))).isEqualTo(Optional.of(Bytes.ofUtf8("v2")));

        // GC: old owner deletes, new owner finishes.
        assertThat(g1.deleted(2, List.of(2, 3))).isFalse();
        apply(g1, new ShardOp.DeleteShards(2, List.of(2, 3)));
        assertThat(g1.status(2)).isEqualTo(ShardStatus.NOT_OWNED);
        assertThat(g1.shardData(2).isPresent()).isFalse();
        assertThat(g1.deleted(2, List.of(2, 3))).isTrue();
        assertThat(g1.allSettled()).isTrue();
        apply(g2, new ShardOp.GcDone(2, List.of(2, 3)));
        assertThat(g2.status(2)).isEqualTo(ShardStatus.SERVING);
        assertThat(g2.allSettled()).isTrue();
    }

    @Test
    void configurationsAreNeverSkipped() {
        apply(g1, new ShardOp.ConfigChange(config(2, 1, 1, 1, 1)));
        assertThat(g1.current().num()).isEqualTo(0L);
        apply(g1, new ShardOp.ConfigChange(config(1, 1, 1, 1, 1)));
        apply(g1, new ShardOp.ConfigChange(config(1, 1, 1, 1, 1))); // duplicate
        assertThat(g1.current().num()).isEqualTo(1L);
    }

    @Test
    void aSnapshotTakenMidMigrationResumesIt() {
        moveHalfToGroup2();
        ShardKVStateMachine restored = new ShardKVStateMachine(2, N);
        restored.restore(g2.snapshot());
        assertThat(restored.digest()).isEqualTo(g2.digest());
        assertThat(restored.status(2)).isEqualTo(ShardStatus.PULLING);
        assertThat(restored.previous().num()).isEqualTo(1L);
        apply(restored, new ShardOp.InsertShards(2, g1.frozenShards(2, List.of(2, 3)).orElseThrow()));
        assertThat(restored.peek(keyIn(2))).isEqualTo(Optional.of(Bytes.ofUtf8("v2")));
        ShardKVStateMachine oldOwner = new ShardKVStateMachine(1, N);
        oldOwner.restore(g1.snapshot());
        assertThat(oldOwner.frozenShards(2, List.of(2)).isPresent()).as("frozen data survives a snapshot").isTrue();
    }

    @Test
    void garbageEntriesNeverThrow() {
        assertThat(KVCodec.decodeOutcome(g1.apply(1, Bytes.copyOf(new byte[] {11, 1, 2}))).status())
                .isEqualTo(ClientStatus.INVALID_REQUEST);
        assertThat(KVCodec.decodeOutcome(g1.apply(2, Bytes.EMPTY)).status()).isEqualTo(ClientStatus.INVALID_REQUEST);
    }
}
