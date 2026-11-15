package dev.raftkv.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.codec.Frame;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;

/**
 * Converts {@link Message}s to payload bytes and back.
 *
 * <p>Both directions use exhaustive switches (pattern matching over the sealed interface for
 * encoding, over the enum for decoding), so a new message type cannot be added without the
 * compiler forcing an update here.
 *
 * <p>Decoding never trusts the input: any malformed payload raises {@link CodecException},
 * never another exception type, and the record constructors re-check every invariant.
 */
public final class MessageCodec {

    /** Smallest possible encoded LogEntry: index(8) + term(8) + type(1) + data length(4). */
    private static final int MIN_ENTRY_BYTES = 8 + 8 + 1 + 4;

    private MessageCodec() {
    }

    // ------------------------------------------------------------------ encoding

    public static Bytes encode(Message message) {
        BinaryWriter w = new BinaryWriter();
        switch (message) {
            case RequestVote m -> {
                w.writeLong(m.term());
                writeNodeId(w, m.candidateId());
                w.writeLong(m.lastLogIndex());
                w.writeLong(m.lastLogTerm());
                w.writeBoolean(m.preVote());
                w.writeBoolean(m.leadershipTransfer());
            }
            case RequestVoteReply m -> {
                w.writeLong(m.term());
                w.writeBoolean(m.voteGranted());
                w.writeBoolean(m.preVote());
            }
            case AppendEntries m -> {
                w.writeLong(m.term());
                writeNodeId(w, m.leaderId());
                w.writeLong(m.prevLogIndex());
                w.writeLong(m.prevLogTerm());
                w.writeLong(m.leaderCommit());
                w.writeLong(m.round());
                w.writeInt(m.entries().size());
                for (LogEntry e : m.entries()) {
                    writeEntry(w, e);
                }
            }
            case AppendEntriesReply m -> {
                w.writeLong(m.term());
                w.writeBoolean(m.success());
                w.writeLong(m.matchIndex());
                w.writeLong(m.conflictIndex());
                w.writeLong(m.conflictTerm());
                w.writeLong(m.round());
            }
            case InstallSnapshot m -> {
                w.writeLong(m.term());
                writeNodeId(w, m.leaderId());
                w.writeLong(m.lastIncludedIndex());
                w.writeLong(m.lastIncludedTerm());
                w.writeLong(m.offset());
                w.writeBytes(m.data());
                w.writeBoolean(m.done());
            }
            case InstallSnapshotReply m -> {
                w.writeLong(m.term());
                w.writeLong(m.lastIncludedIndex());
                w.writeLong(m.nextOffset());
                w.writeBoolean(m.installed());
            }
            case TimeoutNow m -> {
                w.writeLong(m.term());
                writeNodeId(w, m.leaderId());
            }
            case ClientRequest m -> {
                w.writeLong(m.clientId().getMostSignificantBits());
                w.writeLong(m.clientId().getLeastSignificantBits());
                w.writeLong(m.sequence());
                w.writeLong(m.ackedBelow());
                w.writeBytes(m.command());
            }
            case ClientReply m -> {
                w.writeLong(m.clientId().getMostSignificantBits());
                w.writeLong(m.clientId().getLeastSignificantBits());
                w.writeLong(m.sequence());
                w.writeByte(m.status().code());
                w.writeBoolean(m.leaderHint() != null);
                w.writeInt(m.leaderHint() != null ? m.leaderHint().value() : 0);
                w.writeBytes(m.result());
            }
            case FetchShard m -> {
                w.writeLong(m.configNum());
                w.writeInt(m.requesterGroup());
                writeShards(w, m.shards());
            }
            case FetchShardReply m -> {
                w.writeLong(m.configNum());
                w.writeBoolean(m.ready());
                w.writeBytes(m.shards());
            }
            case DeleteShard m -> {
                w.writeLong(m.configNum());
                w.writeInt(m.requesterGroup());
                writeShards(w, m.shards());
            }
            case DeleteShardReply m -> {
                w.writeLong(m.configNum());
                w.writeBoolean(m.done());
                writeShards(w, m.shards());
            }
        }
        return w.toBytes();
    }

    /** Wraps a message in a frame for the given group. */
    public static Frame toFrame(GroupId group, long correlationId, Message message) {
        return new Frame(message.type().code(), group.value(), correlationId, (byte) 0, encode(message));
    }

    // ------------------------------------------------------------------ decoding

    public static Message decode(MessageType type, Bytes payload) {
        BinaryReader r = new BinaryReader(payload);
        Message message;
        try {
            message = switch (type) {
                case REQUEST_VOTE -> new RequestVote(r.readLong(), readNodeId(r), r.readLong(), r.readLong(),
                        r.readBoolean(), r.readBoolean());
                case REQUEST_VOTE_REPLY -> new RequestVoteReply(r.readLong(), r.readBoolean(), r.readBoolean());
                case APPEND_ENTRIES -> decodeAppendEntries(r);
                case APPEND_ENTRIES_REPLY -> new AppendEntriesReply(r.readLong(), r.readBoolean(), r.readLong(),
                        r.readLong(), r.readLong(), r.readLong());
                case INSTALL_SNAPSHOT -> new InstallSnapshot(r.readLong(), readNodeId(r), r.readLong(), r.readLong(),
                        r.readLong(), r.readBytes(), r.readBoolean());
                case INSTALL_SNAPSHOT_REPLY -> new InstallSnapshotReply(r.readLong(), r.readLong(), r.readLong(), r.readBoolean());
                case TIMEOUT_NOW -> new TimeoutNow(r.readLong(), readNodeId(r));
                case CLIENT_REQUEST -> new ClientRequest(new UUID(r.readLong(), r.readLong()), r.readLong(), r.readLong(),
                        r.readBytes());
                case CLIENT_REPLY -> decodeClientReply(r);
                case FETCH_SHARD -> new FetchShard(r.readLong(), r.readInt(), readShards(r));
                case FETCH_SHARD_REPLY -> new FetchShardReply(r.readLong(), r.readBoolean(), r.readBytes());
                case DELETE_SHARD -> new DeleteShard(r.readLong(), r.readInt(), readShards(r));
                case DELETE_SHARD_REPLY -> new DeleteShardReply(r.readLong(), r.readBoolean(), readShards(r));
            };
        } catch (IllegalArgumentException e) {
            // A record constructor rejected the values: the bytes were well-formed but meaningless.
            throw new CodecException("invalid " + type + ": " + e.getMessage());
        }
        r.expectEnd();
        return message;
    }

    /** Decodes the message inside a frame. */
    public static Message fromFrame(Frame frame) {
        return decode(MessageType.fromCode(frame.type()), frame.payload());
    }

    // Java evaluates constructor arguments left to right (JLS §15.12.4.2), which is what makes
    // the one-line decoders above read the fields in the order they were written.

    private static ClientReply decodeClientReply(BinaryReader r) {
        UUID clientId = new UUID(r.readLong(), r.readLong());
        long sequence = r.readLong();
        ClientStatus status = ClientStatus.fromCode(r.readByte());
        boolean hasHint = r.readBoolean();
        int hint = r.readInt();
        return new ClientReply(clientId, sequence, status, hasHint ? new NodeId(hint) : null, r.readBytes());
    }

    private static AppendEntries decodeAppendEntries(BinaryReader r) {
        long term = r.readLong();
        NodeId leader = readNodeId(r);
        long prevLogIndex = r.readLong();
        long prevLogTerm = r.readLong();
        long leaderCommit = r.readLong();
        long round = r.readLong();
        int count = r.readInt();
        // Never allocate based on an untrusted count: each entry needs at least MIN_ENTRY_BYTES.
        if (count < 0 || count > r.remaining() / MIN_ENTRY_BYTES) {
            throw new CodecException("invalid entry count " + count + " with " + r.remaining() + " bytes left");
        }
        List<LogEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(readEntry(r));
        }
        return new AppendEntries(term, leader, prevLogIndex, prevLogTerm, entries, leaderCommit, round);
    }

    // ------------------------------------------------------------------ shared pieces

    private static void writeShards(BinaryWriter w, List<Integer> shards) {
        w.writeInt(shards.size());
        for (int s : shards) {
            w.writeInt(s);
        }
    }

    private static List<Integer> readShards(BinaryReader r) {
        int count = r.readInt();
        if (count < 0 || count > r.remaining() / 4) {
            throw new CodecException("invalid shard count " + count);
        }
        List<Integer> shards = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            shards.add(r.readInt());
        }
        return shards;
    }

    private static void writeNodeId(BinaryWriter w, NodeId id) {
        w.writeInt(id.value());
    }

    private static NodeId readNodeId(BinaryReader r) {
        return new NodeId(r.readInt());
    }

    /** Also used by the on-disk log format in Milestone 4. */
    static void writeEntry(BinaryWriter w, LogEntry e) {
        w.writeLong(e.index());
        w.writeLong(e.term());
        w.writeByte(e.type().code());
        w.writeBytes(e.data());
    }

    static LogEntry readEntry(BinaryReader r) {
        return new LogEntry(r.readLong(), r.readLong(), EntryType.fromCode(r.readByte()), r.readBytes());
    }
}
