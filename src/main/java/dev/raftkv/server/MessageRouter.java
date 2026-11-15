package dev.raftkv.server;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.transport.ClientRegistry;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.MessageHandler;
import dev.raftkv.transport.Transport;

/**
 * Routes every incoming message of one node (SPEC §6.2, multi-Raft): a {@link ClientReply} to the
 * local client whose id it echoes (config pollers, admin clients), everything else to the replica
 * of the group named in the envelope, on that replica's event loop. A client request for a group
 * this node doesn't host gets WRONG_GROUP. The same router runs in the simulator and in real
 * processes, so the routing logic is tested under chaos.
 */
public final class MessageRouter implements MessageHandler, ClientRegistry {

    private final Map<GroupId, GroupReplica> groups = new ConcurrentHashMap<>();
    private final Map<UUID, MessageHandler> clients = new ConcurrentHashMap<>();
    private volatile Transport transport;

    /** The transport used for WRONG_GROUP replies. */
    public void attach(Transport t) {
        this.transport = Objects.requireNonNull(t, "transport");
    }

    public void add(GroupReplica replica) {
        if (groups.putIfAbsent(replica.group(), replica) != null) {
            throw new IllegalStateException(replica.group() + " is already hosted here");
        }
    }

    public void remove(GroupId group) {
        groups.remove(group);
    }

    public Optional<GroupReplica> replica(GroupId group) {
        return Optional.ofNullable(groups.get(group));
    }

    public Map<GroupId, GroupReplica> replicas() {
        return Map.copyOf(groups);
    }

    @Override
    public void register(UUID clientId, MessageHandler handler) {
        clients.put(clientId, handler);
    }

    @Override
    public void onMessage(Envelope envelope) {
        if (envelope.message() instanceof ClientReply reply) {
            MessageHandler client = clients.get(reply.clientId());
            if (client != null) {
                client.onMessage(envelope);
            }
            return;
        }
        GroupReplica replica = groups.get(envelope.group());
        if (replica == null) {
            Transport t = transport;
            if (envelope.message() instanceof ClientRequest request && t != null) {
                t.send(envelope.from(), envelope.group(), ClientReply.to(request, ClientStatus.WRONG_GROUP, null, Bytes.EMPTY));
            }
            return;
        }
        replica.deliver(envelope);
    }
}
