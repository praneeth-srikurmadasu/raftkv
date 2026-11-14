package dev.raftkv.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.Cancellable;
import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Scheduler;
import dev.raftkv.kv.KVCodec;
import dev.raftkv.kv.KVCommand;
import dev.raftkv.kv.KVResult;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * The RaftKV client library (SPEC §7).
 *
 * <p>What it does for you:
 * <ul>
 *   <li><b>Leader discovery:</b> it starts with any server; a NOT_LEADER reply carries a hint,
 *       and it remembers the leader for later requests.</li>
 *   <li><b>Retries:</b> on timeout or NOT_LEADER it retries, with exponential backoff and full
 *       jitter, until the operation's deadline.</li>
 *   <li><b>Exactly-once:</b> every operation gets a sequence number in this client's session, and
 *       every retry reuses it. The cluster's session table makes sure a retried PUT or CAS
 *       takes effect once, even if an earlier attempt also committed.</li>
 *   <li><b>Many operations in flight:</b> the async methods can be called concurrently; the
 *       client tracks which replies it has received and tells the servers (ack watermark).</li>
 * </ul>
 *
 * <p>Threading: all internal state lives on the given {@link Scheduler} (an event loop). The
 * public methods only schedule work, so they can be called from any thread. The blocking
 * convenience methods ({@code put}, {@code get}, ...) must NOT be called from that event loop.
 */
public final class RaftKVClient {

    private final Transport transport;
    private final Scheduler scheduler;
    private final Clock clock;
    private final Random random;
    private final ClientConfig config;
    private final List<NodeId> servers;
    private final GroupId group;
    private final UUID clientId;

    private long nextSequence = 1;
    private final TreeMap<Long, Pending> outstanding = new TreeMap<>();
    private NodeId leader;
    private int roundRobin;
    private long retries;

    private final class Pending {
        final long sequence;
        final Bytes command;
        final CompletableFuture<KVResult> future = new CompletableFuture<>();
        final long deadlineNanos;
        int attempts;
        Cancellable timer;

        Pending(long sequence, Bytes command, long deadlineNanos) {
            this.sequence = sequence;
            this.command = command;
            this.deadlineNanos = deadlineNanos;
        }
    }

    /**
     * @param transport this client's transport (its own node id on the network)
     * @param servers   the servers of the group; any of them works as a starting point
     */
    public RaftKVClient(Transport transport, Scheduler scheduler, Clock clock, Random random,
                        List<NodeId> servers, GroupId group, ClientConfig config) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.servers = List.copyOf(servers);
        if (this.servers.isEmpty()) {
            throw new IllegalArgumentException("need at least one server");
        }
        this.group = Objects.requireNonNull(group, "group");
        this.config = Objects.requireNonNull(config, "config");
        this.clientId = new UUID(random.nextLong(), random.nextLong());
    }

    // =====================================================================================
    // Public API (async)
    // =====================================================================================

    /** Sets {@code key}; completes with the previous value, if any. */
    public CompletableFuture<Optional<Bytes>> putAsync(Bytes key, Bytes value) {
        return submit(new KVCommand.Put(key, value)).thenApply(r -> ((KVResult.PutResult) r).previous());
    }

    /** Linearizable read of {@code key}. */
    public CompletableFuture<Optional<Bytes>> getAsync(Bytes key) {
        return submit(new KVCommand.Get(key)).thenApply(r -> ((KVResult.GetResult) r).value());
    }

    /** Removes {@code key}; completes with whether it existed. */
    public CompletableFuture<Boolean> deleteAsync(Bytes key) {
        return submit(new KVCommand.Delete(key)).thenApply(r -> ((KVResult.DeleteResult) r).existed());
    }

    /** Compare-and-set ({@code expected} empty = only if absent). */
    public CompletableFuture<KVResult.CasResult> casAsync(Bytes key, Optional<Bytes> expected, Bytes newValue) {
        return submit(new KVCommand.Cas(key, expected, newValue)).thenApply(r -> (KVResult.CasResult) r);
    }

    // ---- Blocking conveniences with String keys/values (never call these on the event loop).

    public Optional<String> put(String key, String value) {
        return putAsync(Bytes.ofUtf8(key), Bytes.ofUtf8(value)).join().map(Bytes::toUtf8);
    }

    public Optional<String> get(String key) {
        return getAsync(Bytes.ofUtf8(key)).join().map(Bytes::toUtf8);
    }

    public boolean delete(String key) {
        return deleteAsync(Bytes.ofUtf8(key)).join();
    }

    public boolean cas(String key, String expected, String newValue) {
        return casAsync(Bytes.ofUtf8(key), Optional.ofNullable(expected).map(Bytes::ofUtf8), Bytes.ofUtf8(newValue)).join().success();
    }

    /** Feed every message addressed to this client here (from the transport). */
    public void onMessage(Envelope envelope) {
        scheduler.execute(() -> handleReply(envelope));
    }

    public UUID clientId() {
        return clientId;
    }

    /** The server this client currently believes is leader, if any. */
    public Optional<NodeId> knownLeader() {
        return Optional.ofNullable(leader);
    }

    /** Retries sent so far (tests, metrics). */
    public long retries() {
        return retries;
    }

    // =====================================================================================
    // Internals (event loop only)
    // =====================================================================================

    private CompletableFuture<KVResult> submit(KVCommand command) {
        Bytes encoded = KVCodec.encode(command); // validates sizes on the caller's thread
        CompletableFuture<KVResult> result = new CompletableFuture<>();
        scheduler.execute(() -> {
            Pending p = new Pending(nextSequence++, encoded, clock.nanos() + config.deadline().toNanos());
            outstanding.put(p.sequence, p);
            p.future.whenComplete((r, e) -> {
                if (e != null) {
                    result.completeExceptionally(e);
                } else {
                    result.complete(r);
                }
            });
            send(p);
        });
        return result;
    }

    /** The lowest sequence number we haven't received a reply for: everything below is acknowledged. */
    private long ackedBelow() {
        return outstanding.isEmpty() ? nextSequence : outstanding.firstKey();
    }

    private void send(Pending p) {
        if (p.future.isDone()) {
            return;
        }
        if (clock.nanos() >= p.deadlineNanos) {
            finish(p);
            p.future.completeExceptionally(new TimeoutException("no answer within " + config.deadline()
                    + " after " + p.attempts + " attempts"));
            return;
        }
        NodeId target = leader != null ? leader : servers.get(Math.floorMod(roundRobin++, servers.size()));
        p.attempts++;
        transport.send(target, group, new ClientRequest(clientId, p.sequence, ackedBelow(), p.command));
        cancel(p.timer);
        p.timer = scheduler.schedule(config.attemptTimeout(), () -> {
            if (!p.future.isDone()) {
                leader = null; // the guess didn't answer: try the others
                retry(p);
            }
        });
    }

    private void retry(Pending p) {
        retries++;
        cancel(p.timer);
        // Exponential backoff with full jitter: random in [0, min(max, base * 2^attempts)].
        long base = config.backoffBase().toNanos();
        long cap = Math.min(config.backoffMax().toNanos(), base << Math.min(p.attempts, 20));
        long delay = cap <= 0 ? 0 : random.nextLong(cap + 1);
        p.timer = scheduler.schedule(Duration.ofNanos(delay), () -> send(p));
    }

    private void handleReply(Envelope envelope) {
        if (!(envelope.message() instanceof ClientReply reply)) {
            return;
        }
        Pending p = outstanding.get(reply.sequence());
        if (p == null || p.future.isDone()) {
            return; // a duplicate reply, or one for a request that already finished
        }
        switch (reply.status()) {
            case OK -> {
                leader = envelope.from();
                finish(p);
                p.future.complete(KVCodec.decodeResult(reply.result()));
            }
            case NOT_LEADER -> {
                leader = reply.leaderHintOptional().filter(h -> !h.equals(envelope.from())).orElse(null);
                retry(p);
            }
            case SESSION_EXPIRED -> {
                finish(p);
                p.future.completeExceptionally(new SessionExpiredException("session " + clientId + " expired; outcome of request "
                        + p.sequence + " is unknown"));
            }
            case INVALID_REQUEST -> {
                finish(p);
                p.future.completeExceptionally(new IllegalArgumentException("server rejected request " + p.sequence + " as invalid"));
            }
            case WRONG_GROUP, SHARD_NOT_READY, OVERLOADED -> retry(p); // sharding and load shedding arrive later
        }
    }

    private void finish(Pending p) {
        cancel(p.timer);
        outstanding.remove(p.sequence);
    }

    private static void cancel(Cancellable c) {
        if (c != null) {
            c.cancel();
        }
    }

    /** Sequence numbers of operations still in flight (tests). */
    List<Long> outstandingSequences() {
        return new ArrayList<>(outstanding.keySet());
    }
}
