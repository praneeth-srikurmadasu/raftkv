package dev.raftkv.client;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
 * The client of a single (unsharded) RaftKV group (Milestone 5): leader discovery, retries with
 * backoff, exactly-once sessions and many operations in flight, all from {@link SessionClient}.
 * The sharded cluster's client is {@code dev.raftkv.shard.ShardedKVClient}.
 *
 * <p>Threading: all internal state lives on the given {@link Scheduler} (an event loop). The
 * public methods only schedule work, so they can be called from any thread. The blocking
 * convenience methods ({@code put}, {@code get}, ...) must NOT be called from that event loop.
 */
public final class RaftKVClient implements KVClient {

    private final SessionClient session;
    private final GroupId group;

    /**
     * @param transport this client's transport (its own node id on the network)
     * @param servers   the servers of the group; any of them works as a starting point
     */
    public RaftKVClient(Transport transport, Scheduler scheduler, Clock clock, Random random,
                        List<NodeId> servers, GroupId group, ClientConfig config) {
        this.group = group;
        this.session = new SessionClient(transport, scheduler, clock, random, Router.fixed(new Route(group, servers)), config);
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

    /** Feed every message addressed to this client here (from the transport). */
    public void onMessage(Envelope envelope) {
        session.onMessage(envelope);
    }

    public UUID clientId() {
        return session.clientId();
    }

    /** The server this client currently believes is leader, if any. */
    public Optional<NodeId> knownLeader() {
        return session.knownLeader(group);
    }

    /** Retries sent so far (tests, metrics). */
    public long retries() {
        return session.retries();
    }

    /** Sequence numbers of operations still in flight (tests). */
    List<Long> outstandingSequences() {
        return session.outstandingSequences();
    }

    private CompletableFuture<KVResult> submit(KVCommand command) {
        Bytes encoded = KVCodec.encode(command); // validates sizes on the caller's thread
        return session.submit(encoded, command.key(), command instanceof KVCommand.Get).thenApply(KVCodec::decodeResult);
    }
}
