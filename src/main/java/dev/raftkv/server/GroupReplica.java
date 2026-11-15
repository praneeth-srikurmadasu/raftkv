package dev.raftkv.server;

import java.util.Objects;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.Scheduler;
import dev.raftkv.protocol.RaftMessage;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.StateMachine;
import dev.raftkv.raft.log.RaftLog;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.MessageHandler;

/**
 * One node's replica of one Raft group: the {@link RaftNode}, the state machine it drives, and the
 * service that handles everything that isn't a Raft message (client requests, shard moves).
 * Built by {@link Replicas}. Every method except {@link #deliver} must run on {@link #scheduler()}.
 */
public final class GroupReplica {

    private final GroupId group;
    private final Scheduler scheduler;
    private final RaftNode node;
    private final StateMachine stateMachine;
    private final RaftLog log;
    private final MessageHandler service;
    private final Runnable onStart;
    private final Runnable onStop;

    GroupReplica(GroupId group, Scheduler scheduler, RaftNode node, StateMachine stateMachine, RaftLog log,
                 MessageHandler service, Runnable onStart, Runnable onStop) {
        this.group = Objects.requireNonNull(group, "group");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.node = Objects.requireNonNull(node, "node");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.log = Objects.requireNonNull(log, "log");
        this.service = Objects.requireNonNull(service, "service");
        this.onStart = Objects.requireNonNull(onStart, "onStart");
        this.onStop = Objects.requireNonNull(onStop, "onStop");
    }

    /** Hands a message to this replica's event loop (any thread). */
    public void deliver(Envelope envelope) {
        scheduler.execute(() -> {
            if (!node.isRunning()) {
                return;
            }
            if (envelope.message() instanceof RaftMessage) {
                node.handle(envelope);
            } else {
                service.onMessage(envelope);
            }
        });
    }

    /** Recovers from storage and starts the Raft node and the service's background work. */
    public void start() {
        node.start();
        onStart.run();
    }

    /** Stops everything, like a crash: persistent state stays in the stores. */
    public void stop() {
        onStop.run();
        node.stop();
    }

    public GroupId group() {
        return group;
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public RaftNode node() {
        return node;
    }

    public StateMachine stateMachine() {
        return stateMachine;
    }

    public RaftLog log() {
        return log;
    }

    public MessageHandler service() {
        return service;
    }
}
