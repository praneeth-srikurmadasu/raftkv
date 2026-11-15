package dev.raftkv.client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import dev.raftkv.server.Admin;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * Sends {@link Admin} commands (status, add/remove server, transfer leadership) to a group and
 * returns the text answer. Membership changes go to the group's leader (following NOT_LEADER
 * hints, rotating through servers on silence); status asks one specific server. Every method may
 * be called from any thread; state lives on the given scheduler. Feed every incoming message to
 * {@link #onMessage}: replies for other clients are ignored.
 */
public final class AdminClient {

    private static final Duration RETRY = Duration.ofMillis(300);

    private final Transport transport;
    private final Scheduler scheduler;
    private final Clock clock;
    private final List<NodeId> servers;
    private final UUID clientId = UUID.randomUUID();
    private final Map<Long, Pending> pending = new TreeMap<>();
    private long nextSequence = 1;

    private final class Pending {
        final long sequence;
        final GroupId group;
        final Bytes command;
        final boolean fixedTarget;
        final long deadlineNanos;
        final CompletableFuture<String> future = new CompletableFuture<>();
        NodeId target;
        int rotation;
        Cancellable timer;

        Pending(long sequence, GroupId group, Bytes command, NodeId target, boolean fixedTarget, long deadlineNanos) {
            this.sequence = sequence;
            this.group = group;
            this.command = command;
            this.target = target;
            this.fixedTarget = fixedTarget;
            this.deadlineNanos = deadlineNanos;
        }
    }

    public AdminClient(Transport transport, Scheduler scheduler, Clock clock, List<NodeId> servers) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.servers = List.copyOf(servers);
        if (this.servers.isEmpty()) {
            throw new IllegalArgumentException("need at least one server");
        }
    }

    /** The status line of {@code group}'s replica on {@code server}. */
    public CompletableFuture<String> status(GroupId group, NodeId server, Duration deadline) {
        return submit(group, new Admin.Command(Admin.Op.STATUS, null), server, true, deadline);
    }

    /** Adds {@code server} to {@code group} (as a learner, then promoted once caught up). */
    public CompletableFuture<String> addServer(GroupId group, NodeId server, Duration deadline) {
        return submit(group, new Admin.Command(Admin.Op.ADD_SERVER, server), servers.get(0), false, deadline);
    }

    public CompletableFuture<String> removeServer(GroupId group, NodeId server, Duration deadline) {
        return submit(group, new Admin.Command(Admin.Op.REMOVE_SERVER, server), servers.get(0), false, deadline);
    }

    public CompletableFuture<String> transferLeadership(GroupId group, NodeId server, Duration deadline) {
        return submit(group, new Admin.Command(Admin.Op.TRANSFER_LEADER, server), servers.get(0), false, deadline);
    }

    private CompletableFuture<String> submit(GroupId group, Admin.Command c, NodeId first, boolean fixed, Duration deadline) {
        CompletableFuture<String> result = new CompletableFuture<>();
        scheduler.execute(() -> {
            Pending p = new Pending(nextSequence++, group, Admin.encode(c), first, fixed, clock.nanos() + deadline.toNanos());
            pending.put(p.sequence, p);
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

    private void send(Pending p) {
        if (p.future.isDone()) {
            return;
        }
        if (clock.nanos() >= p.deadlineNanos) {
            finish(p);
            p.future.completeExceptionally(new TimeoutException("no answer from group " + p.group + " in time"));
            return;
        }
        transport.send(p.target, p.group, new ClientRequest(clientId, p.sequence, 1, p.command));
        cancel(p.timer);
        p.timer = scheduler.schedule(RETRY, () -> {
            if (!p.fixedTarget) {
                p.rotation++;
                p.target = servers.get(p.rotation % servers.size());
            }
            send(p);
        });
    }

    /** Handles a message from the transport (any thread). */
    public void onMessage(Envelope envelope) {
        if (envelope.message() instanceof ClientReply reply && reply.clientId().equals(clientId)) {
            scheduler.execute(() -> handleReply(envelope, reply));
        }
    }

    private void handleReply(Envelope envelope, ClientReply reply) {
        Pending p = pending.get(reply.sequence());
        if (p == null || p.future.isDone()) {
            return;
        }
        String text = new String(reply.result().toArray(), StandardCharsets.UTF_8);
        switch (reply.status()) {
            case OK -> {
                finish(p);
                p.future.complete(text);
            }
            case NOT_LEADER -> {
                if (p.fixedTarget) {
                    finish(p);
                    p.future.completeExceptionally(new IllegalStateException(text));
                    return;
                }
                NodeId hint = reply.leaderHintOptional().filter(h -> !h.equals(envelope.from())).orElse(null);
                if (hint != null) {
                    p.target = hint;
                } else {
                    p.rotation++;
                    p.target = servers.get(p.rotation % servers.size());
                }
                cancel(p.timer);
                p.timer = scheduler.schedule(Duration.ofMillis(20), () -> send(p));
            }
            default -> {
                finish(p);
                p.future.completeExceptionally(new IllegalStateException(text.isEmpty() ? reply.status().toString() : text));
            }
        }
    }

    private void finish(Pending p) {
        cancel(p.timer);
        pending.remove(p.sequence);
    }

    private static void cancel(Cancellable c) {
        if (c != null) {
            c.cancel();
        }
    }
}
