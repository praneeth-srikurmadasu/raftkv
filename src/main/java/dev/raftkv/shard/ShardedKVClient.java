package dev.raftkv.shard;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.KVClient;
import dev.raftkv.client.Route;
import dev.raftkv.client.Router;
import dev.raftkv.client.SessionClient;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Scheduler;
import dev.raftkv.kv.KVCodec;
import dev.raftkv.kv.KVCommand;
import dev.raftkv.kv.KVResult;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * The client of the sharded cluster (SPEC §7): routes each key to its shard, the shard to its
 * group with a cached configuration, and the request to that group's leader.
 *
 * <ul>
 *   <li>It starts knowing only the controller's servers and fetches the latest configuration.</li>
 *   <li>WRONG_GROUP (the shard moved) refreshes the configuration and retries; SHARD_NOT_READY (the
 *       new owner is still pulling) backs off and retries the same group.</li>
 *   <li>Every retry keeps its sequence number, to any group: the session table travels with the
 *       shard, so a write that took effect at the old owner is answered from the cache at the new
 *       one instead of running twice.</li>
 * </ul>
 * Threading as for every client: state lives on the given scheduler.
 */
public final class ShardedKVClient implements KVClient {

    private final ControllerClient controller;
    private final SessionClient session;
    private ShardConfig config;
    private boolean refreshing;

    public ShardedKVClient(Transport transport, Scheduler scheduler, Clock clock, Random random,
                           List<NodeId> controllerServers, ClientConfig clientConfig) {
        this.controller = new ControllerClient(transport, scheduler, clock, random, controllerServers, clientConfig);
        this.session = new SessionClient(transport, scheduler, clock, random, new ShardRouter(), clientConfig);
    }

    /** Routes by the cached configuration; refreshes it from the controller when told it's stale. */
    private final class ShardRouter implements Router {
        @Override
        public Optional<Route> route(Bytes key) {
            ShardConfig c = config;
            if (c == null) {
                return Optional.empty();
            }
            int gid = c.gidOf(Shards.shardOf(key, c.numShards()));
            if (gid == 0) {
                return Optional.empty();
            }
            return Optional.of(new Route(GroupId.of(gid), c.servers(gid)));
        }

        @Override
        public void refresh() {
            if (refreshing) {
                return;
            }
            refreshing = true;
            controller.query(ControllerCommand.Query.LATEST).whenComplete((c, e) -> {
                refreshing = false;
                if (c != null && (config == null || c.num() > config.num())) {
                    config = c;
                }
            });
        }
    }

    @Override
    public CompletableFuture<Optional<Bytes>> putAsync(Bytes key, Bytes value) {
        return submit(new KVCommand.Put(key, value)).thenApply(r -> ((KVResult.PutResult) r).previous());
    }

    @Override
    public CompletableFuture<Optional<Bytes>> getAsync(Bytes key) {
        return submit(new KVCommand.Get(key)).thenApply(r -> ((KVResult.GetResult) r).value());
    }

    @Override
    public CompletableFuture<Boolean> deleteAsync(Bytes key) {
        return submit(new KVCommand.Delete(key)).thenApply(r -> ((KVResult.DeleteResult) r).existed());
    }

    @Override
    public CompletableFuture<KVResult.CasResult> casAsync(Bytes key, Optional<Bytes> expected, Bytes newValue) {
        return submit(new KVCommand.Cas(key, expected, newValue)).thenApply(r -> (KVResult.CasResult) r);
    }

    /** Feed every message addressed to this client's node here. */
    public void onMessage(Envelope envelope) {
        session.onMessage(envelope);
        controller.onMessage(envelope);
    }

    /** For admin commands (Join, Leave, Move, Query). */
    public ControllerClient controller() {
        return controller;
    }

    /** The configuration this client routes by (null before the first fetch). Read on the client's loop. */
    public ShardConfig cachedConfig() {
        return config;
    }

    /** The server this client believes leads {@code gid}. */
    public Optional<NodeId> knownLeader(int gid) {
        return session.knownLeader(GroupId.of(gid));
    }

    public UUID clientId() {
        return session.clientId();
    }

    public long retries() {
        return session.retries();
    }

    private CompletableFuture<KVResult> submit(KVCommand command) {
        Bytes encoded = KVCodec.encode(command);
        return session.submit(encoded, command.key(), command instanceof KVCommand.Get).thenApply(KVCodec::decodeResult);
    }
}
