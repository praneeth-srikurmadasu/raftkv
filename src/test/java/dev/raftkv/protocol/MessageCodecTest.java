package dev.raftkv.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.codec.Frame;
import dev.raftkv.codec.FrameCodec;
import dev.raftkv.codec.FrameDecoder;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

class MessageCodecTest {

    private static final NodeId N1 = NodeId.of(1);

    /** One hand-written example of every message type. */
    static List<Message> examples() {
        return List.of(
                new RequestVote(5, N1, 10, 4, false, false),
                new RequestVote(6, N1, 0, 0, true, true),
                new RequestVoteReply(5, true, false),
                new AppendEntries(3, N1, 7, 2, List.of(), 6),
                new AppendEntries(3, N1, 7, 2, List.of(
                        new LogEntry(8, 3, EntryType.COMMAND, Bytes.ofUtf8("PUT city Hyderabad")),
                        LogEntry.noop(9, 3),
                        new LogEntry(10, 3, EntryType.CONFIGURATION, Bytes.copyOf(new byte[] {1, 2}))), 9),
                new AppendEntriesReply(3, false, 0, 4, 2),
                new InstallSnapshot(9, N1, 1000, 8, 65536, Bytes.copyOf(new byte[100]), true),
                new InstallSnapshotReply(9, 1000, 65636, false),
                new TimeoutNow(4, N1),
                new ClientRequest(new java.util.UUID(1, 2), 7, 5, Bytes.ofUtf8("PUT a b")),
                new ClientReply(new java.util.UUID(3, 4), 7, ClientStatus.OK, null, Bytes.ofUtf8("result")),
                new ClientReply(new java.util.UUID(3, 4), 8, ClientStatus.NOT_LEADER, N1, Bytes.EMPTY),
                new FetchShard(4, 2, java.util.List.of(0, 7, 63)),
                new FetchShardReply(4, true, Bytes.ofUtf8("shard bytes")),
                new DeleteShard(4, 2, java.util.List.of(7)),
                new DeleteShardReply(4, true, java.util.List.of()));
    }

    @Test
    void everyExampleRoundTrips() {
        for (Message m : examples()) {
            assertThat(MessageCodec.decode(m.type(), MessageCodec.encode(m))).isEqualTo(m);
        }
    }

    @Test
    void examplesCoverEveryMessageType() {
        Set<MessageType> covered = new HashSet<>();
        examples().forEach(m -> covered.add(m.type()));
        assertThat(covered).isEqualTo(Set.of(MessageType.values()));
    }

    /** Property: any valid message survives encode -> frame -> bytes -> frame -> decode. */
    @Property(tries = 2000)
    void anyMessageRoundTripsThroughAFrame(@ForAll long seed) {
        Message original = RandomMessages.any(new Random(seed));
        GroupId group = GroupId.of(new Random(seed).nextInt(100));
        byte[] wire = FrameCodec.encodeToArray(MessageCodec.toFrame(group, seed, original));

        List<Frame> frames = new FrameDecoder().feed(wire);
        assertThat(frames).hasSize(1);
        assertThat(frames.get(0).groupId()).isEqualTo(group.value());
        assertThat(frames.get(0).correlationId()).isEqualTo(seed);
        assertThat(MessageCodec.fromFrame(frames.get(0))).as("seed %d", seed).isEqualTo(original);
    }

    /** Every strict prefix of a valid payload is truncated data and must be rejected cleanly. */
    @Test
    void everyTruncationIsRejected() {
        List<Message> samples = new ArrayList<>(examples());
        Random random = new Random(7);
        for (int i = 0; i < 50; i++) {
            samples.add(RandomMessages.any(random));
        }
        for (Message m : samples) {
            byte[] full = MessageCodec.encode(m).toArray();
            for (int cut = 0; cut < full.length; cut++) {
                Bytes prefix = Bytes.copyOf(Arrays.copyOf(full, cut));
                int at = cut;
                assertThatThrownBy(() -> MessageCodec.decode(m.type(), prefix))
                        .as("%s cut at %d of %d", m.type(), at, full.length)
                        .isInstanceOf(CodecException.class);
            }
        }
    }

    @Test
    void trailingBytesAreRejected() {
        Message m = new InstallSnapshotReply(1, 2, 3, true);
        byte[] full = MessageCodec.encode(m).toArray();
        Bytes longer = Bytes.copyOf(Arrays.copyOf(full, full.length + 1));
        assertThatThrownBy(() -> MessageCodec.decode(m.type(), longer))
                .isInstanceOf(CodecException.class).hasMessageContaining("trailing");
    }

    @Test
    void unknownTypeCodeIsRejected() {
        Frame frame = new Frame((short) 999, 0, 0, (byte) 0, Bytes.EMPTY);
        assertThatThrownBy(() -> MessageCodec.fromFrame(frame))
                .isInstanceOf(CodecException.class).hasMessageContaining("unknown message type");
    }

    @Test
    void lyingEntryCountDoesNotAllocate() {
        Bytes payload = new BinaryWriter()
                .writeLong(1).writeInt(1).writeLong(0).writeLong(0).writeLong(0) // term, leader, prev idx/term, commit
                .writeLong(0)                                                    // heartbeat round
                .writeInt(Integer.MAX_VALUE)                                     // "2 billion entries follow"
                .toBytes();
        assertThatThrownBy(() -> MessageCodec.decode(MessageType.APPEND_ENTRIES, payload))
                .isInstanceOf(CodecException.class).hasMessageContaining("invalid entry count");
    }

    @Test
    void wellFormedButInvalidValuesBecomeCodecExceptions() {
        Bytes negativeTerm = new BinaryWriter().writeLong(-5).writeLong(0).writeLong(0).writeBoolean(false).toBytes();
        assertThatThrownBy(() -> MessageCodec.decode(MessageType.INSTALL_SNAPSHOT_REPLY, negativeTerm))
                .isInstanceOf(CodecException.class).hasMessageContaining("term");

        Bytes badEntryType = new BinaryWriter()
                .writeLong(1).writeInt(1).writeLong(0).writeLong(0).writeLong(0).writeLong(0).writeInt(1)
                .writeLong(1).writeLong(1).writeByte(42).writeBytes(Bytes.EMPTY)
                .toBytes();
        assertThatThrownBy(() -> MessageCodec.decode(MessageType.APPEND_ENTRIES, badEntryType))
                .isInstanceOf(CodecException.class).hasMessageContaining("entry type");
    }

    @Test
    void appendEntriesRequiresConsecutiveIndexes() {
        List<LogEntry> gap = List.of(LogEntry.noop(5, 1), LogEntry.noop(7, 1));
        assertThatThrownBy(() -> new AppendEntries(1, N1, 4, 1, gap, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("expected 6");
        assertThatThrownBy(() -> new AppendEntries(1, N1, 3, 1, List.of(LogEntry.noop(5, 1)), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void appendEntriesKeepsAnImmutableCopy() {
        List<LogEntry> entries = new ArrayList<>(List.of(LogEntry.noop(1, 1)));
        AppendEntries m = new AppendEntries(1, N1, 0, 0, entries, 0);
        entries.add(LogEntry.noop(2, 1));
        assertThat(m.entries()).hasSize(1);
        assertThatThrownBy(() -> m.entries().add(LogEntry.noop(2, 1))).isInstanceOf(UnsupportedOperationException.class);
    }

    /** Wire codes are a protocol contract: this test fails if anyone renumbers them. */
    @Test
    void wireCodesAreStable() {
        assertThat(MessageType.REQUEST_VOTE.code()).isEqualTo((short) 1);
        assertThat(MessageType.REQUEST_VOTE_REPLY.code()).isEqualTo((short) 2);
        assertThat(MessageType.APPEND_ENTRIES.code()).isEqualTo((short) 3);
        assertThat(MessageType.APPEND_ENTRIES_REPLY.code()).isEqualTo((short) 4);
        assertThat(MessageType.INSTALL_SNAPSHOT.code()).isEqualTo((short) 5);
        assertThat(MessageType.INSTALL_SNAPSHOT_REPLY.code()).isEqualTo((short) 6);
        assertThat(MessageType.TIMEOUT_NOW.code()).isEqualTo((short) 7);
        assertThat(MessageType.CLIENT_REQUEST.code()).isEqualTo((short) 100);
        assertThat(MessageType.CLIENT_REPLY.code()).isEqualTo((short) 101);
        assertThat(MessageType.FETCH_SHARD.code()).isEqualTo((short) 110);
        assertThat(MessageType.FETCH_SHARD_REPLY.code()).isEqualTo((short) 111);
        assertThat(MessageType.DELETE_SHARD.code()).isEqualTo((short) 112);
        assertThat(MessageType.DELETE_SHARD_REPLY.code()).isEqualTo((short) 113);
        assertThat(ClientStatus.WRONG_GROUP.code()).isEqualTo((byte) 4);
        assertThat(ClientStatus.SHARD_NOT_READY.code()).isEqualTo((byte) 5);
        assertThat(ClientStatus.OK.code()).isEqualTo((byte) 0);
        assertThat(ClientStatus.NOT_LEADER.code()).isEqualTo((byte) 1);
        assertThat(ClientStatus.SESSION_EXPIRED.code()).isEqualTo((byte) 2);
        assertThat(ClientStatus.INVALID_REQUEST.code()).isEqualTo((byte) 3);
        assertThat(EntryType.COMMAND.code()).isEqualTo((byte) 1);
        assertThat(EntryType.NOOP.code()).isEqualTo((byte) 2);
        assertThat(EntryType.CONFIGURATION.code()).isEqualTo((byte) 3);
        for (MessageType t : MessageType.values()) {
            assertThat(MessageType.fromCode(t.code())).isEqualTo(t);
        }
    }
}
