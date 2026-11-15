package dev.raftkv.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * The machinery every RaftKV client shares (SPEC §7): one session, sequence numbers, the ack
 * watermark, leader discovery per group, and retries.
 *
 * <ul>
 *   <li><b>Exactly-once:</b> every request gets the next sequence number of this session, and every
 *       retry, to whichever server or group, reuses it. The servers' session tables make sure it
 *       takes effect once.</li>
 *   <li><b>Routing:</b> a {@link Router} picks the group for each attempt, so a request whose shard
 *       moved goes to the new owner on the next attempt.</li>
 *   <li><b>Leader discovery:</b> the leader of each group is remembered; NOT_LEADER replies carry a
 *       hint; a server that doesn't answer in time is skipped next time.</li>
 *   <li><b>Retry policy by status:</b> NOT_LEADER follows the hint; WRONG_GROUP asks the router to
 *       refresh and retries; SHARD_NOT_READY backs off and retries the same group; timeouts rotate
 *       servers. Backoff is exponential with full jitter, until the request's deadline.</li>
 *   <li><b>Many requests in flight:</b> the watermark ({@code ackedBelow}) is the lowest sequence
 *       number without a reply, so servers know which cached results they may drop.</li>
 * </ul>
 *
 * Threading: all state lives on the given {@link Scheduler}; {@link #submit} and
 * {@link #onMessage} only schedule work, so they can be called from any thread.
 */
public final class SessionClient {

    private final Transport transport;
    private final Scheduler scheduler;
    private final Clock clock;
    private final Random random;
    private final Router router;
    private final ClientConfig config;
    private final UUID clientId;

    private long nextSequence = 1;
    private final TreeMap<Long, Pending> outstanding = new TreeMap<>();
    private final Map<GroupId, NodeId> leaders = new HashMap<>();
    private final Map<GroupId, Integer> roundRobin = new HashMap<>();
    private long retries;
    /** True once any request of this session succeeded, i.e. the servers know the session. */
    private boolean sessionKnown;

    private final class Pending {
        final long sequence;
        final Bytes command;
        final Bytes routingKey;
        final CompletableFuture<Bytes> future = new CompletableFuture<>();
        final long deadlineNanos;
        int attempts;
        Cancellable timer;

        Pending(long sequence, Bytes command, Bytes routingKey, long deadlineNanos) {
            this.sequence = sequence;
            this.command = command;
            this.routingKey = routingKey;
            this.deadlineNanos = deadlineNanos;
        }
    }

    public SessionClient(Transport transport, Scheduler scheduler, Clock clock, Random random, Router router,
                         ClientConfig config) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.router = Objects.requireNonNull(router, "router");
        this.config = Objects.requireNonNull(config, "config");
        this.clientId = new UUID(random.nextLong(), random.nextLong());
    }

    /**
     * Sends {@code command} (routed by {@code routingKey}) until it succeeds or the deadline passes.
     * Completes with the operation's result bytes; fails with {@link TimeoutException},
     * {@link SessionExpiredException}, or {@link IllegalArgumentException} (the server rejected it).
     */
    public CompletableFuture<Bytes> submit(Bytes command, Bytes routingKey) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(routingKey, "routingKey");
        CompletableFuture<Bytes> result = new CompletableFuture<>();
        scheduler.execute(() -> {
            Pending p = new Pending(nextSequence++, command, routingKey, clock.nanos() + config.deadline().toNanos());
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

    /** Feed every message addressed to this client here (from the transport). */
    public void onMessage(Envelope envelope) {
        scheduler.execute(() -> handleReply(envelope));
    }

    public UUID clientId() {
        return clientId;
    }

    /** The server this client believes leads {@code group}, if any. */
    public Optional<NodeId> knownLeader(GroupId group) {
        return Optional.ofNullable(leaders.get(group));
    }

    /** Retries sent so far (tests, metrics). */
    public long retries() {
        return retries;
    }

    /** Sequence numbers of requests still in flight (tests). */
    public List<Long> outstandingSequences() {
        return new ArrayList<>(outstanding.keySet());
    }

    // =====================================================================================
    // Internals (event loop only)
    // =====================================================================================

    /**
     * The lowest sequence number we haven't received a reply for: everything below is acknowledged.
     *
     * <p>Until the first reply arrives we send 1. A server that has never heard of a client with a
     * watermark above 1 concludes that it once had the session and has since expired it (so the
     * request may already have run) and refuses it. If our first requests timed out without ever
     * reaching a server, the watermark would already be above 1 and every later request would be
     * refused forever. (Found by the sharded chaos test: a config poller whose first queries were
     * lost in a partition could never talk to the controller again.)
     */
    private long ackedBelow() {
        if (!sessionKnown) {
            return 1;
        }
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
        p.attempts++;
        Optional<Route> route = router.route(p.routingKey);
        if (route.isEmpty()) {
            router.refresh(); // e.g. no configuration yet: ask for one, try again shortly
            retry(p);
            return;
        }
        GroupId group = route.get().group();
        List<NodeId> servers = route.get().servers();
        NodeId leader = leaders.get(group);
        NodeId target;
        if (leader != null) {
            target = leader; // may not be in the list the client started with: a hint led us there
        } else {
            int next = roundRobin.merge(group, 1, Integer::sum);
            target = servers.get(Math.floorMod(next, servers.size()));
        }
        transport.send(target, group, new ClientRequest(clientId, p.sequence, ackedBelow(), p.command));
        cancel(p.timer);
        p.timer = scheduler.schedule(config.attemptTimeout(), () -> {
            if (!p.future.isDone()) {
                leaders.remove(group); // the guess didn't answer: try the others
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
        if (!(envelope.message() instanceof ClientReply reply) || !reply.clientId().equals(clientId)) {
            return;
        }
        Pending p = outstanding.get(reply.sequence());
        if (p == null || p.future.isDone()) {
            return; // a duplicate reply, or one for a request that already finished
        }
        GroupId group = envelope.group();
        switch (reply.status()) {
            case OK -> {
                sessionKnown = true;
                leaders.put(group, envelope.from());
                finish(p);
                p.future.complete(reply.result());
            }
            case NOT_LEADER -> {
                NodeId hint = reply.leaderHintOptional().filter(h -> !h.equals(envelope.from())).orElse(null);
                if (hint != null) {
                    leaders.put(group, hint);
                } else {
                    leaders.remove(group);
                }
                retry(p);
            }
            case WRONG_GROUP -> {
                leaders.put(group, envelope.from()); // it answered, so it's alive; just not the owner
                router.refresh();
                retry(p);
            }
            case SHARD_NOT_READY, OVERLOADED -> retry(p);
            case SESSION_EXPIRED -> {
                finish(p);
                p.future.completeExceptionally(new SessionExpiredException("session " + clientId
                        + " expired; outcome of request " + p.sequence + " is unknown"));
            }
            case INVALID_REQUEST -> {
                finish(p);
                p.future.completeExceptionally(new IllegalArgumentException("server rejected request "
                        + p.sequence + " as invalid"));
            }
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
}
