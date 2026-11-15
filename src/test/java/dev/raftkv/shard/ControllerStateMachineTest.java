package dev.raftkv.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVCodec;
import dev.raftkv.kv.SessionCommand;
import dev.raftkv.protocol.ClientStatus;
import org.junit.jupiter.api.Test;

class ControllerStateMachineTest {

    private static final UUID ADMIN = new UUID(7, 7);
    private final ControllerStateMachine sm = new ControllerStateMachine(10);
    private long index;
    private long seq;

    private KVCodec.Outcome run(ControllerCommand c) {
        return run(ADMIN, ++seq, c);
    }

    private KVCodec.Outcome run(UUID client, long sequence, ControllerCommand c) {
        Bytes payload = KVCodec.encode(new SessionCommand(client, sequence, 1, 1000, ControllerCodec.encode(c)));
        return KVCodec.decodeOutcome(sm.apply(++index, payload));
    }

    private static ShardConfig config(KVCodec.Outcome o) {
        assertThat(o.status()).isEqualTo(ClientStatus.OK);
        return ControllerCodec.decodeConfig(o.result());
    }

    private static TreeMap<Integer, List<NodeId>> group(int gid, int... nodes) {
        TreeMap<Integer, List<NodeId>> m = new TreeMap<>();
        List<NodeId> list = new java.util.ArrayList<>();
        for (int n : nodes) {
            list.add(NodeId.of(n));
        }
        m.put(gid, list);
        return m;
    }

    @Test
    void startsWithConfigZeroAndNoGroups() {
        ShardConfig c = config(run(new ControllerCommand.Query(-1)));
        assertThat(c.num()).isEqualTo(0L);
        assertThat(c.groups().isEmpty()).isTrue();
        assertThat(c.shardsOf(0).size()).isEqualTo(10);
    }

    @Test
    void joinLeaveMoveCreateNumberedConfigs() {
        ShardConfig c1 = config(run(new ControllerCommand.Join(group(1, 1, 2, 3))));
        assertThat(c1.num()).isEqualTo(1L);
        assertThat(c1.shardsOf(1).size()).isEqualTo(10);
        ShardConfig c2 = config(run(new ControllerCommand.Join(group(2, 4, 5, 6))));
        assertThat(c2.shardsOf(2).size()).isEqualTo(5);
        ShardConfig c3 = config(run(new ControllerCommand.Move(9, 1)));
        assertThat(c3.gidOf(9)).isEqualTo(1);
        assertThat(Rebalancer.moves(c2.assignment(), c3.assignment())).as("move touches one shard").isLessThanOrEqualTo(1);
        ShardConfig c4 = config(run(new ControllerCommand.Leave(List.of(2))));
        assertThat(c4.shardsOf(1).size()).isEqualTo(10);
        assertThat(c4.groups().containsKey(2)).isFalse();
        assertThat(config(run(new ControllerCommand.Query(2)))).isEqualTo(c2);
        assertThat(config(run(new ControllerCommand.Query(1000)))).isEqualTo(c4);
        assertThat(sm.history().size()).isEqualTo(5);
    }

    @Test
    void invalidOperationsAreRefusedAndChangeNothing() {
        config(run(new ControllerCommand.Join(group(1, 1))));
        assertThat(run(new ControllerCommand.Leave(List.of(1))).status()).as("never remove the last group")
                .isEqualTo(ClientStatus.INVALID_REQUEST);
        assertThat(run(new ControllerCommand.Leave(List.of(9))).status()).isEqualTo(ClientStatus.INVALID_REQUEST);
        assertThat(run(new ControllerCommand.Move(10, 1)).status()).isEqualTo(ClientStatus.INVALID_REQUEST);
        assertThat(run(new ControllerCommand.Move(0, 5)).status()).isEqualTo(ClientStatus.INVALID_REQUEST);
        assertThat(sm.latest().num()).isEqualTo(1L);
    }

    @Test
    void aRetriedJoinCreatesOneConfiguration() {
        KVCodec.Outcome first = run(ADMIN, 1, new ControllerCommand.Join(group(1, 1)));
        KVCodec.Outcome retry = run(ADMIN, 1, new ControllerCommand.Join(group(1, 1)));
        assertThat(retry).isEqualTo(first);
        assertThat(sm.latest().num()).isEqualTo(1L);
    }

    @Test
    void snapshotsRestoreTheWholeHistory() {
        run(new ControllerCommand.Join(group(1, 1, 2, 3)));
        run(new ControllerCommand.Join(group(2, 4)));
        ControllerStateMachine copy = new ControllerStateMachine(10);
        copy.restore(sm.snapshot());
        assertThat(copy.history()).isEqualTo(sm.history());
        assertThat(copy.digest()).isEqualTo(sm.digest());
    }
}
