package dev.raftkv.shard;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVResult;
import dev.raftkv.linearizability.History;
import dev.raftkv.linearizability.LinearizabilityChecker;
import dev.raftkv.linearizability.Operation;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.ReadMode;
import dev.raftkv.sim.SimulationException;

/**
 * The Milestone 10 chaos run (SPEC §9.2-9.5, §11): a sharded cluster (controller + 3 data groups)
 * under swarm faults, with clients doing random operations while an admin keeps reconfiguring the
 * cluster: Join, Leave and Move, so shards migrate all the time.
 *
 * <p>Checked: the sharding invariants after every event ({@link ShardInvariants}); at the end, after
 * healing, that the cluster settles, every shard lives exactly where the final configuration puts
 * it ({@link ShardedCluster#checkSettledOwnership}), and that the whole history, including a final
 * read of every key, is linearizable.
 */
final class ShardedChaos {

    static final int MAX_REPLACEMENTS = 3;
    static java.util.function.Consumer<ShardedCluster> DEBUG = c -> { };
    static final int GROUPS = 3;

    private ShardedChaos() {
    }

    /** @param definitelyNotLinearizable the checker PROVED a violation (not just ran out of budget) */
    record Result(long seed, Optional<String> failure, int operations, int unknown, int configs, boolean definitelyNotLinearizable) {
        boolean passed() {
            return failure.isEmpty();
        }
    }

    private static final class Worker {
        final int id;
        final ShardedKVClient client;
        final Random random;
        final Map<Bytes, Optional<Bytes>> lastSeen = new HashMap<>();
        int counter;
        CompletableFuture<?> inFlight = CompletableFuture.completedFuture(null);

        Worker(int id, ShardedKVClient client, Random random) {
            this.id = id;
            this.client = client;
            this.random = random;
        }
    }

    private static final class Workload {
        final ShardedCluster cluster;
        final List<Bytes> keys;
        final History history;
        final ClientConfig clientConfig;
        final List<Worker> workers = new ArrayList<>();
        boolean issuing = true;
        int replacements;

        Workload(ShardedCluster cluster, List<Bytes> keys, History history, ClientConfig clientConfig) {
            this.cluster = cluster;
            this.keys = keys;
            this.history = history;
            this.clientConfig = clientConfig;
        }

        void spawn() {
            Worker w = new Worker(workers.size(), cluster.newClient(clientConfig), cluster.sim.forkRandom());
            workers.add(w);
            cluster.sim.execute(() -> next(this, w));
        }
    }

    static Result run(long seed, RaftConfig baseConfig, ShardedCluster.Storage storage) {
        Random r = new Random(seed);
        int numShards = 4 + r.nextInt(9);
        boolean colocated = r.nextInt(3) == 0;
        RaftConfig config = r.nextBoolean() ? baseConfig : baseConfig.withSnapshots(10 + r.nextInt(70), 256 + r.nextInt(4000));
        // Milestone 11 swarm (separate stream, so the choices above stay as they were): read mode,
        // pipelining window, pre-vote and check-quorum (always on with leases).
        Random m11 = new Random(seed ^ 0x11_5EED_C0DEL);
        ReadMode mode = ReadMode.values()[m11.nextInt(3)];
        config = config.withReadMode(mode)
                .withMaxInflightAppends(new int[] {1, 2, 8}[m11.nextInt(3)])
                .withElectionExtensions(m11.nextInt(4) != 0, mode == ReadMode.LEASE || m11.nextInt(4) != 0);
        ShardedCluster c = new ShardedCluster(seed, GROUPS, numShards, config, storage, colocated);
        History history = new History(c.sim::now);
        int configs = 0;
        try {
            boolean partitions = r.nextInt(3) != 0;
            boolean crashes = r.nextInt(3) != 0;
            boolean spikes = r.nextInt(4) == 0;
            int maxLatency = 3 + r.nextInt(20);
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(maxLatency));
            c.net.setDropRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.1);
            c.net.setDuplicateRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.1);
            c.startAll();

            ClientConfig adminConfig = new ClientConfig(Duration.ofMillis(300), Duration.ofMillis(10),
                    Duration.ofMillis(300), Duration.ofSeconds(20));
            ControllerClient admin = c.newAdmin(adminConfig);
            List<Integer> joined = new ArrayList<>(List.of(1));
            if (r.nextBoolean()) {
                joined.add(2);
            }
            c.await(admin.join(c.groupsFor(joined.stream().mapToInt(Integer::intValue).toArray())), Duration.ofSeconds(30));

            int keyCount = 3 + r.nextInt(6);
            List<Bytes> keys = new ArrayList<>();
            for (int i = 0; i < keyCount; i++) {
                keys.add(Bytes.ofUtf8("key" + i));
            }
            ClientConfig cfg = new ClientConfig(Duration.ofMillis(100 + r.nextInt(250)), Duration.ofMillis(5),
                    Duration.ofMillis(300), Duration.ofSeconds(4 + r.nextInt(6)));
            Workload load = new Workload(c, keys, history, cfg);
            int clients = 3 + r.nextInt(3);
            for (int i = 0; i < clients; i++) {
                load.spawn();
            }

            Random nemesis = c.sim.forkRandom();
            CompletableFuture<ShardConfig> adminOp = CompletableFuture.completedFuture(null);
            long chaosEnd = c.sim.now() + Duration.ofSeconds(8).toNanos();
            while (c.sim.now() < chaosEnd) {
                c.run(Duration.ofMillis(100 + nemesis.nextInt(400)));
                // Reconfigure (one admin operation at a time, like a careful operator).
                if (adminOp.isDone() && nemesis.nextInt(3) == 0) {
                    adminOp = reconfigure(c, admin, nemesis, joined);
                    configs++;
                }
                act(c, nemesis, partitions, crashes, spikes, maxLatency);
            }

            // ---- recovery
            load.issuing = false;
            c.net.heal();
            c.crashedNodes().forEach(c::restart);
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(maxLatency));
            c.sim.runUntil(() -> load.workers.stream().allMatch(w -> w.inFlight.isDone()), Duration.ofSeconds(60));
            CompletableFuture<ShardConfig> lastAdmin = adminOp;
            c.sim.runUntil(lastAdmin::isDone, Duration.ofSeconds(60));
            if (Boolean.getBoolean("raftkv.debugShard") && !c.sim.runUntil(c::quiescent, Duration.ofSeconds(120))) {
                DEBUG.accept(c);
            }
            c.awaitQuiescent(Duration.ofSeconds(120));
            c.checkSettledOwnership();

            // ---- final read of every key (part of the checked history: lost data shows up here)
            ShardedKVClient reader = c.newClient(new ClientConfig(Duration.ofMillis(300), Duration.ofMillis(10),
                    Duration.ofMillis(300), Duration.ofSeconds(30)));
            for (Bytes key : keys) {
                int h = history.invoke(9999, Operation.Kind.GET, key, Optional.empty(), Optional.empty());
                Optional<Bytes> v = c.await(reader.getAsync(key), Duration.ofSeconds(40));
                history.ok(h, new KVResult.GetResult(v));
            }

            List<Operation> ops = history.operations();
            int unknown = (int) ops.stream().filter(Operation::isUnknown).count();
            LinearizabilityChecker.Result check = LinearizabilityChecker.check(ops);
            if (!check.linearizable()) {
                return new Result(seed, Optional.of((check.inconclusive() ? "INCONCLUSIVE: " : "") + check.report()),
                        ops.size(), unknown, configs, !check.inconclusive());
            }
            return new Result(seed, Optional.empty(), ops.size(), unknown, configs, false);
        } catch (AssertionError | SimulationException | IllegalStateException e) {
            String message = e.getMessage() + (e.getCause() != null ? " / cause: " + e.getCause().getMessage() : "");
            return new Result(seed, Optional.of(message), history.size(), 0, configs, false);
        }
    }

    /** One random Join, Leave or Move. Leave never removes the last group. */
    private static CompletableFuture<ShardConfig> reconfigure(ShardedCluster c, ControllerClient admin, Random r,
                                                              List<Integer> joined) {
        int pick = r.nextInt(3);
        List<Integer> absent = new ArrayList<>();
        for (int gid : c.gids()) {
            if (!joined.contains(gid)) {
                absent.add(gid);
            }
        }
        if (pick == 0 && !absent.isEmpty()) {
            int gid = absent.get(r.nextInt(absent.size()));
            joined.add(gid);
            return admin.join(c.groupsFor(gid));
        }
        if (pick == 1 && joined.size() > 1) {
            Integer gid = joined.get(r.nextInt(joined.size()));
            joined.remove(gid);
            return admin.leave(List.of(gid));
        }
        int gid = joined.get(r.nextInt(joined.size()));
        return admin.move(r.nextInt(c.numShards), gid);
    }

    private static void act(ShardedCluster c, Random r, boolean partitions, boolean crashes, boolean spikes, int maxLatency) {
        int choice = r.nextInt(8);
        List<NodeId> nodes = c.allNodes;
        if (partitions && choice == 0) {
            List<NodeId> shuffled = new ArrayList<>(nodes);
            Collections.shuffle(shuffled, r);
            c.net.partition(shuffled.subList(0, 1 + r.nextInt(nodes.size() / 2)));
        } else if (choice == 1) {
            c.net.heal();
        } else if (crashes && choice == 2 && c.crashedNodes().size() < 2) {
            NodeId victim = nodes.get(r.nextInt(nodes.size()));
            if (c.isRunning(victim)) {
                c.crash(victim);
            }
        } else if (crashes && (choice == 3 || choice == 4)) {
            List<NodeId> crashed = c.crashedNodes();
            if (!crashed.isEmpty()) {
                c.restart(crashed.get(r.nextInt(crashed.size())));
            }
        } else if (spikes && choice == 5) {
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(r.nextBoolean() ? 120 : maxLatency));
        } else if (crashes && choice == 6 && c.crashedNodes().size() < 2) {
            // SPEC §6.7: the leader of a group that is pulling shards crashes mid-migration.
            for (int gid : c.gids()) {
                var leader = c.leader(dev.raftkv.common.GroupId.of(gid));
                if (leader.isPresent() && leader.get().isRunning()) {
                    var sm = (ShardKVStateMachine) c.replica(leader.get().id(), dev.raftkv.common.GroupId.of(gid))
                            .orElseThrow().stateMachine();
                    if (!sm.shardsIn(ShardStatus.PULLING).isEmpty()) {
                        c.crash(leader.get().id());
                        break;
                    }
                }
            }
        }
    }

    /** Issues the worker's next random operation and records it. */
    private static void next(Workload load, Worker w) {
        if (!load.issuing) {
            return;
        }
        ShardedCluster c = load.cluster;
        History history = load.history;
        Bytes key = load.keys.get(w.random.nextInt(load.keys.size()));
        Bytes value = Bytes.ofUtf8("c" + w.id + "-" + (++w.counter));
        int roll = w.random.nextInt(100);
        CompletableFuture<?> f;
        if (roll < 35) {
            int h = history.invoke(w.id, Operation.Kind.PUT, key, Optional.of(value), Optional.empty());
            f = w.client.putAsync(key, value).thenAccept(prev -> {
                history.ok(h, new KVResult.PutResult(prev));
                w.lastSeen.put(key, Optional.of(value));
            });
        } else if (roll < 70) {
            int h = history.invoke(w.id, Operation.Kind.GET, key, Optional.empty(), Optional.empty());
            f = w.client.getAsync(key).thenAccept(v -> {
                history.ok(h, new KVResult.GetResult(v));
                w.lastSeen.put(key, v);
            });
        } else if (roll < 90) {
            Optional<Bytes> expected = w.lastSeen.getOrDefault(key, Optional.empty());
            int h = history.invoke(w.id, Operation.Kind.CAS, key, Optional.of(value), expected);
            f = w.client.casAsync(key, expected, value).thenAccept(res -> {
                history.ok(h, res);
                w.lastSeen.put(key, res.current());
            });
        } else {
            int h = history.invoke(w.id, Operation.Kind.DELETE, key, Optional.empty(), Optional.empty());
            f = w.client.deleteAsync(key).thenAccept(existed -> {
                history.ok(h, new KVResult.DeleteResult(existed));
                w.lastSeen.put(key, Optional.empty());
            });
        }
        w.inFlight = f;
        f.whenComplete((ok, err) -> {
            if (err == null) {
                c.sim.schedule(Duration.ofMillis(w.random.nextInt(21)), () -> next(load, w));
            } else if (load.replacements < MAX_REPLACEMENTS) {
                load.replacements++;
                load.spawn();
            }
        });
    }
}
