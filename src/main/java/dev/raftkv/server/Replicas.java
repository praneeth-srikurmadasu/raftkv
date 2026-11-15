package dev.raftkv.server;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Scheduler;
import dev.raftkv.common.Tracer;
import dev.raftkv.kv.KVServer;
import dev.raftkv.kv.KVStateMachine;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.raft.StateMachine;
import dev.raftkv.raft.log.RaftLog;
import dev.raftkv.raft.storage.MetaStore;
import dev.raftkv.raft.storage.SnapshotStore;
import dev.raftkv.shard.ControllerCodec;
import dev.raftkv.shard.ControllerStateMachine;
import dev.raftkv.shard.ShardKVServer;
import dev.raftkv.shard.ShardKVStateMachine;
import dev.raftkv.transport.ClientRegistry;
import dev.raftkv.transport.Transport;

/**
 * Builds the three kinds of group replica a node can host: a single-group KV store (Milestones
 * 5–8), the shard controller (group 0), and a data group of the sharded cluster.
 */
public final class Replicas {

    private Replicas() {
    }

    /**
     * Everything a replica needs from its node: identity, timing, network and storage.
     *
     * @param stateMachineWrapper lets tests wrap the state machine (e.g. to record applies); identity in production
     */
    public record Parts(NodeId self, GroupId group, Set<NodeId> voters, RaftConfig config, Scheduler scheduler,
                        Clock clock, Random random, Transport transport, RaftLog log, MetaStore metaStore,
                        SnapshotStore snapshotStore, Tracer tracer, java.util.function.UnaryOperator<StateMachine> stateMachineWrapper) {

        public Parts {
            Objects.requireNonNull(self, "self");
            Objects.requireNonNull(group, "group");
            Objects.requireNonNull(voters, "voters");
            Objects.requireNonNull(config, "config");
            Objects.requireNonNull(scheduler, "scheduler");
            Objects.requireNonNull(clock, "clock");
            Objects.requireNonNull(random, "random");
            Objects.requireNonNull(transport, "transport");
            Objects.requireNonNull(log, "log");
            Objects.requireNonNull(metaStore, "metaStore");
            Objects.requireNonNull(snapshotStore, "snapshotStore");
            Objects.requireNonNull(tracer, "tracer");
            Objects.requireNonNull(stateMachineWrapper, "stateMachineWrapper");
        }

        /** The same parts without a wrapper. */
        public Parts(NodeId self, GroupId group, Set<NodeId> voters, RaftConfig config, Scheduler scheduler,
                     Clock clock, Random random, Transport transport, RaftLog log, MetaStore metaStore,
                     SnapshotStore snapshotStore, Tracer tracer) {
            this(self, group, voters, config, scheduler, clock, random, transport, log, metaStore, snapshotStore, tracer,
                    sm -> sm);
        }

        RaftNode node(StateMachine sm) {
            return RaftNode.builder()
                    .self(self).group(group).voters(voters).config(config)
                    .scheduler(scheduler).clock(clock).random(random).transport(transport)
                    .log(log).metaStore(metaStore).snapshotStore(snapshotStore)
                    .stateMachine(sm).tracer(tracer)
                    .build();
        }
    }

    /** A replica of a single, unsharded KV group. */
    public static GroupReplica kv(Parts p, Duration sessionTimeout) {
        KVStateMachine store = new KVStateMachine(sessionTimeout, p.config().bugs());
        StateMachine sm = p.stateMachineWrapper().apply(store);
        RaftNode node = p.node(sm);
        KVServer server = new KVServer(node, p.transport(), p.clock(), store);
        return new GroupReplica(p.group(), p.scheduler(), node, store, p.log(), server::handle, () -> { }, () -> { });
    }

    /** A replica of the shard controller (group 0). */
    public static GroupReplica controller(Parts p, int numShards, Duration sessionTimeout) {
        if (!p.group().equals(GroupId.CONTROLLER)) {
            throw new IllegalArgumentException("the controller is group 0, not " + p.group());
        }
        ControllerStateMachine store = new ControllerStateMachine(numShards, sessionTimeout);
        StateMachine sm = p.stateMachineWrapper().apply(store);
        RaftNode node = p.node(sm);
        KVServer server = new KVServer(node, p.transport(), p.clock(), null, ControllerCodec::decode);
        return new GroupReplica(p.group(), p.scheduler(), node, store, p.log(), server::handle, () -> { }, () -> { });
    }

    /** A replica of a data group of the sharded cluster. */
    public static GroupReplica shard(Parts p, int numShards, Duration sessionTimeout, List<NodeId> controllerServers,
                                     ClientRegistry registry) {
        ShardKVStateMachine store = new ShardKVStateMachine(p.group().value(), numShards, sessionTimeout, p.config().bugs());
        StateMachine sm = p.stateMachineWrapper().apply(store);
        RaftNode node = p.node(sm);
        ShardKVServer server = new ShardKVServer(node, store, p.transport(), p.clock(), p.scheduler(), p.random(),
                controllerServers, registry);
        return new GroupReplica(p.group(), p.scheduler(), node, store, p.log(), server, server::start, server::stop);
    }
}
