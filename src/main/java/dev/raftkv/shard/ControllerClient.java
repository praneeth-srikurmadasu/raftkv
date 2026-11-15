package dev.raftkv.shard;

import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.Route;
import dev.raftkv.client.Router;
import dev.raftkv.client.SessionClient;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Scheduler;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * Talks to the shard controller (Raft group 0): Query, Join, Leave and Move, exactly once, with
 * leader discovery and retries from {@link SessionClient}. Used by sharded clients (to learn the
 * configuration), by data groups' config pollers, and by the admin commands of the CLI.
 */
public final class ControllerClient {

    /** The controller's Raft group id. */
    public static final GroupId CONTROLLER = GroupId.CONTROLLER;

    private final SessionClient session;

    public ControllerClient(Transport transport, Scheduler scheduler, Clock clock, Random random,
                            List<NodeId> controllerServers, ClientConfig config) {
        this.session = new SessionClient(transport, scheduler, clock, random,
                Router.fixed(new Route(CONTROLLER, controllerServers)), config);
    }

    /** Configuration {@code num}, or the latest with {@link ControllerCommand.Query#LATEST}. */
    public CompletableFuture<ShardConfig> query(long num) {
        return submit(new ControllerCommand.Query(num));
    }

    public CompletableFuture<ShardConfig> join(SortedMap<Integer, List<NodeId>> groups) {
        return submit(new ControllerCommand.Join(groups));
    }

    public CompletableFuture<ShardConfig> leave(List<Integer> gids) {
        return submit(new ControllerCommand.Leave(gids));
    }

    public CompletableFuture<ShardConfig> move(int shard, int gid) {
        return submit(new ControllerCommand.Move(shard, gid));
    }

    /** Feed messages addressed to this client here (replies for other clients are ignored). */
    public void onMessage(Envelope envelope) {
        session.onMessage(envelope);
    }

    public UUID clientId() {
        return session.clientId();
    }

    private CompletableFuture<ShardConfig> submit(ControllerCommand command) {
        return session.submit(ControllerCodec.encode(command), Bytes.EMPTY).thenApply(ControllerCodec::decodeConfig);
    }
}
