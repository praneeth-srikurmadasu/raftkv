package dev.raftkv.server;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVStateMachine;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.raft.NotLeaderException;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.StateMachine;
import dev.raftkv.shard.ControllerStateMachine;
import dev.raftkv.shard.ShardKVStateMachine;
import dev.raftkv.shard.ShardStatus;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * Admin commands for one group (SPEC §3.9, §8): status, add a server, remove a server, transfer
 * leadership. They travel as ordinary client requests whose command starts with {@link #TAG},
 * which no KV or controller command uses, and are answered by the replica itself instead of the
 * group's service. Results and errors come back as UTF-8 text.
 */
public final class Admin {

    /** First byte of every admin command (KV and controller commands use 1-4). */
    public static final byte TAG = 0x7A;

    public enum Op {
        STATUS(1), ADD_SERVER(2), REMOVE_SERVER(3), TRANSFER_LEADER(4);

        final byte code;

        Op(int code) {
            this.code = (byte) code;
        }

        static Op fromCode(byte code) {
            for (Op op : values()) {
                if (op.code == code) {
                    return op;
                }
            }
            throw new CodecException("unknown admin op " + code);
        }
    }

    public record Command(Op op, NodeId target) {
        public Command {
            Objects.requireNonNull(op, "op");
            if (op != Op.STATUS) {
                Objects.requireNonNull(target, "target");
            }
        }
    }

    public static Bytes encode(Command c) {
        BinaryWriter w = new BinaryWriter();
        w.writeByte(TAG);
        w.writeByte(c.op().code);
        w.writeInt(c.target() == null ? -1 : c.target().value());
        return w.toBytes();
    }

    public static Command decode(Bytes b) {
        BinaryReader r = new BinaryReader(b);
        if (r.readByte() != TAG) {
            throw new CodecException("not an admin command");
        }
        Op op = Op.fromCode(r.readByte());
        int target = r.readInt();
        r.expectEnd();
        if (op != Op.STATUS && target < 0) {
            throw new CodecException("admin " + op + " needs a server id");
        }
        return new Command(op, target < 0 ? null : new NodeId(target));
    }

    public static boolean isAdmin(Bytes command) {
        return !command.isEmpty() && command.get(0) == TAG;
    }

    private final GroupId group;
    private final RaftNode node;
    private final StateMachine stateMachine;
    private final Transport transport;

    Admin(GroupId group, RaftNode node, StateMachine stateMachine, Transport transport) {
        this.group = group;
        this.node = node;
        this.stateMachine = stateMachine;
        this.transport = transport;
    }

    /** Handles one admin request (on the replica's event loop). */
    void handle(Envelope envelope, ClientRequest request) {
        Command c;
        try {
            c = decode(request.command());
        } catch (CodecException e) {
            reply(envelope, request, ClientStatus.INVALID_REQUEST, null, e.getMessage());
            return;
        }
        if (c.op() == Op.STATUS) {
            reply(envelope, request, ClientStatus.OK, null, status());
            return;
        }
        CompletableFuture<?> work = switch (c.op()) {
            case ADD_SERVER -> node.addServer(c.target());
            case REMOVE_SERVER -> node.removeServer(c.target());
            case TRANSFER_LEADER -> node.transferLeadership(c.target());
            case STATUS -> throw new IllegalStateException("handled above");
        };
        work.whenComplete((r, error) -> {
            if (error == null) {
                reply(envelope, request, ClientStatus.OK, null, c.op() + " " + c.target() + " done: " + node.membership());
                return;
            }
            Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
            if (cause instanceof NotLeaderException nle) {
                reply(envelope, request, ClientStatus.NOT_LEADER, nle.leaderHint().orElse(null), "not the leader");
            } else {
                reply(envelope, request, ClientStatus.INVALID_REQUEST, null, c.op() + " failed: " + cause.getMessage());
            }
        });
    }

    /** One line describing this replica: role, term, indexes, membership, shard summary. */
    String status() {
        StringBuilder sb = new StringBuilder();
        sb.append(group).append(' ').append(node.id()).append(' ').append(node.role())
                .append(" term=").append(node.currentTerm())
                .append(" leader=").append(node.leaderId().map(NodeId::toString).orElse("?"))
                .append(" commit=").append(node.commitIndex())
                .append(" applied=").append(node.lastApplied())
                .append(" log=").append(node.log().snapshotIndex()).append("..").append(node.log().lastIndex())
                .append(' ').append(node.membership());
        switch (stateMachine) {
            case ControllerStateMachine cs -> sb.append(" config=").append(cs.latest().num());
            case ShardKVStateMachine s -> sb.append(" config=").append(s.current().num())
                    .append(" serving=").append(s.shardsIn(ShardStatus.SERVING).size() + s.shardsIn(ShardStatus.GCING).size())
                    .append(" pulling=").append(s.shardsIn(ShardStatus.PULLING).size())
                    .append(" frozen=").append(s.shardsIn(ShardStatus.BEPULLING).size());
            case KVStateMachine kv -> sb.append(" keys=").append(kv.size());
            default -> { }
        }
        return sb.toString();
    }

    private void reply(Envelope envelope, ClientRequest request, ClientStatus status, NodeId hint, String text) {
        transport.send(envelope.from(), group, ClientReply.to(request, status, hint,
                Bytes.copyOf(text.getBytes(StandardCharsets.UTF_8))));
    }
}
