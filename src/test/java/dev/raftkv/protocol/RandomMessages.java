package dev.raftkv.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;

/**
 * Generates random but valid messages from a seed, for round-trip and fragmentation tests.
 *
 * <p>Values are biased toward edge cases (0, 1, Long.MAX_VALUE, empty data), because that is
 * where encoding bugs hide; plain uniform random longs would almost never hit them.
 */
public final class RandomMessages {

    private RandomMessages() {
    }

    public static Message any(Random r) {
        MessageType[] types = MessageType.values();
        return of(types[r.nextInt(types.length)], r);
    }

    public static Message of(MessageType type, Random r) {
        return switch (type) {
            case REQUEST_VOTE -> new RequestVote(edgeLong(r), node(r), edgeLong(r), edgeLong(r), r.nextBoolean(), r.nextBoolean());
            case REQUEST_VOTE_REPLY -> new RequestVoteReply(edgeLong(r), r.nextBoolean(), r.nextBoolean());
            case APPEND_ENTRIES -> appendEntries(r);
            case APPEND_ENTRIES_REPLY -> new AppendEntriesReply(edgeLong(r), r.nextBoolean(), edgeLong(r), edgeLong(r), edgeLong(r));
            case INSTALL_SNAPSHOT -> new InstallSnapshot(edgeLong(r), node(r), edgeLong(r), edgeLong(r), edgeLong(r), bytes(r, 512), r.nextBoolean());
            case INSTALL_SNAPSHOT_REPLY -> new InstallSnapshotReply(edgeLong(r));
            case TIMEOUT_NOW -> new TimeoutNow(edgeLong(r), node(r));
            case CLIENT_REQUEST -> {
                long seq = 1 + (edgeLong(r) % (Long.MAX_VALUE - 1));
                long acked = 1 + (r.nextLong() & Long.MAX_VALUE) % seq;
                yield new ClientRequest(new java.util.UUID(r.nextLong(), r.nextLong()), seq, acked, bytes(r, 256));
            }
            case CLIENT_REPLY -> new ClientReply(1 + (edgeLong(r) % (Long.MAX_VALUE - 1)),
                    ClientStatus.values()[r.nextInt(ClientStatus.values().length)],
                    r.nextBoolean() ? node(r) : null, bytes(r, 256));
        };
    }

    public static List<Message> list(Random r, int count) {
        List<Message> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(any(r));
        }
        return out;
    }

    private static AppendEntries appendEntries(Random r) {
        long prevLogIndex = r.nextInt(4) == 0 ? 0 : Math.min(edgeLong(r), Long.MAX_VALUE - 1000);
        int count = r.nextInt(4) == 0 ? 0 : r.nextInt(20);
        List<LogEntry> entries = new ArrayList<>(count);
        long term = 1 + r.nextInt(5);
        for (int i = 0; i < count; i++) {
            if (r.nextInt(5) == 0) {
                term += 1 + r.nextInt(3);
            }
            EntryType type = EntryType.values()[r.nextInt(EntryType.values().length)];
            Bytes data = type == EntryType.NOOP ? Bytes.EMPTY : bytes(r, 64);
            entries.add(new LogEntry(prevLogIndex + 1 + i, term, type, data));
        }
        return new AppendEntries(edgeLong(r), node(r), prevLogIndex, edgeLong(r), entries, edgeLong(r));
    }

    /** A non-negative long, often an edge value. */
    static long edgeLong(Random r) {
        return switch (r.nextInt(8)) {
            case 0 -> 0;
            case 1 -> 1;
            case 2 -> Long.MAX_VALUE;
            case 3 -> Long.MAX_VALUE - 1;
            case 4 -> Integer.MAX_VALUE + 1L;
            case 5 -> r.nextInt(100);
            default -> r.nextLong() & Long.MAX_VALUE;
        };
    }

    static NodeId node(Random r) {
        return new NodeId(switch (r.nextInt(4)) {
            case 0 -> 0;
            case 1 -> Integer.MAX_VALUE;
            default -> r.nextInt(10);
        });
    }

    static Bytes bytes(Random r, int maxLength) {
        int length = r.nextInt(4) == 0 ? 0 : r.nextInt(maxLength + 1);
        byte[] b = new byte[length];
        r.nextBytes(b);
        return Bytes.copyOf(b);
    }
}
