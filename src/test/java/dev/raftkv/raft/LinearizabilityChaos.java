package dev.raftkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVResult;
import dev.raftkv.linearizability.History;
import dev.raftkv.linearizability.LinearizabilityChecker;
import dev.raftkv.linearizability.Operation;
import dev.raftkv.sim.SimulationException;

/**
 * The Milestone 6 chaos run (SPEC §9.2-9.5): a swarm of faults, several clients doing random
 * operations on a few shared keys, every operation recorded, and the whole history checked for
 * linearizability at the end.
 *
 * <p>Swarm testing: each seed switches fault types on or off independently (partitions,
 * one-way link failures, crashes, drops, duplicates, latency spikes) and picks their strength.
 * Some runs hammer one kind of failure, others mix everything.
 *
 * <p>Clients have short deadlines, so some operations end with an unknown outcome. The checker
 * has to handle those correctly too, because that is where real systems go wrong. Like Jepsen,
 * a client whose operation timed out is retired (it may still have a request in flight) and
 * replaced by a fresh client with a new session, at most {@link #MAX_REPLACEMENTS} times per run.
 * That keeps the number of open-ended operations small enough for the checker's search.
 */
final class LinearizabilityChaos {

    static final int MAX_REPLACEMENTS = 3;

    private LinearizabilityChaos() {
    }

    /**
     * @param definitelyNotLinearizable true only if the checker PROVED a violation (not merely
     *                                  ran out of search budget)
     */
    record Result(long seed, Optional<String> failure, int operations, int unknown, boolean definitelyNotLinearizable) {
        boolean passed() {
            return failure.isEmpty();
        }
    }

    /** Shared state of one run's workload. */
    private static final class Workload {
        final SimulatedCluster cluster;
        final List<Bytes> keys;
        final History history;
        final List<Worker> workers = new ArrayList<>();
        final ClientConfig clientConfig;
        boolean issuing = true;
        int replacements;

        Workload(SimulatedCluster cluster, List<Bytes> keys, History history, ClientConfig clientConfig) {
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

    private static final class Worker {
        final int id;
        final RaftKVClient client;
        final Random random;
        final Map<Bytes, Optional<Bytes>> lastSeen = new HashMap<>();
        int counter;
        CompletableFuture<?> inFlight = CompletableFuture.completedFuture(null);

        Worker(int id, RaftKVClient client, Random random) {
            this.id = id;
            this.client = client;
            this.random = random;
        }
    }

    static Result run(long seed, RaftConfig config, SimulatedCluster.Storage storage) {
        Random r = new Random(seed);
        SimulatedCluster c = SimulatedCluster.kv(seed, r.nextBoolean() ? 3 : 5, SnapshotSwarm.pick(seed, config), storage);
        History history = new History(c.sim::now);
        try {
            // ---- swarm: which faults are on in this run
            boolean partitions = r.nextInt(3) != 0;
            boolean oneWay = r.nextInt(3) == 0;
            boolean crashes = r.nextInt(3) != 0;
            boolean spikes = r.nextInt(3) == 0;
            int maxLatency = 5 + r.nextInt(26);
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(maxLatency));
            c.net.setDropRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.15);
            c.net.setDuplicateRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.15);
            c.startAll();

            int keyCount = 2 + r.nextInt(3);
            List<Bytes> keys = new ArrayList<>();
            for (int i = 0; i < keyCount; i++) {
                keys.add(Bytes.ofUtf8("k" + i));
            }
            ClientConfig cfg = new ClientConfig(Duration.ofMillis(50 + r.nextInt(250)), Duration.ofMillis(5),
                    Duration.ofMillis(300), Duration.ofSeconds(3 + r.nextInt(6)));
            Workload load = new Workload(c, keys, history, cfg);
            int clients = 3 + r.nextInt(3);
            for (int i = 0; i < clients; i++) {
                load.spawn();
            }
            List<Worker> workers = load.workers;

            Random nemesis = c.sim.forkRandom();
            long chaosEnd = Duration.ofSeconds(10).toNanos();
            while (c.sim.now() < chaosEnd) {
                c.run(Duration.ofMillis(100 + nemesis.nextInt(501)));
                int pick = nemesis.nextInt(10);
                if (oneWay && pick == 0) {
                    NodeId a = c.ids.get(nemesis.nextInt(c.ids.size()));
                    NodeId b = c.ids.get(nemesis.nextInt(c.ids.size()));
                    if (!a.equals(b)) {
                        c.net.blockOneWay(a, b);
                    }
                } else if (spikes && pick == 1) {
                    c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(nemesis.nextBoolean() ? 150 : maxLatency));
                } else {
                    ElectionChaos.act(c, nemesis, partitions, crashes);
                }
            }

            // ---- recovery: stop new operations, heal, let every in-flight one finish or time out
            load.issuing = false;
            c.net.heal();
            c.crashedNodes().forEach(c::restart);
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(maxLatency));
            c.sim.runUntil(() -> workers.stream().allMatch(w -> w.inFlight.isDone()), Duration.ofSeconds(60));
            c.awaitLeader(Duration.ofSeconds(10));

            List<Operation> ops = history.operations();
            int unknown = (int) ops.stream().filter(Operation::isUnknown).count();
            LinearizabilityChecker.Result check = LinearizabilityChecker.check(ops);
            if (!check.linearizable()) {
                // An inconclusive search is a failure too: an unproven history proves nothing.
                return new Result(seed, Optional.of((check.inconclusive() ? "INCONCLUSIVE: " : "") + check.report()),
                        ops.size(), unknown, !check.inconclusive());
            }
            return new Result(seed, Optional.empty(), ops.size(), unknown, false);
        } catch (AssertionError | SimulationException | IllegalStateException e) {
            String message = e.getMessage() + (e.getCause() != null ? " / cause: " + e.getCause().getMessage() : "");
            return new Result(seed, Optional.of(message), history.size(), 0, false);
        }
    }

    /** Issues the worker's next random operation and records it. */
    private static void next(Workload load, Worker w) {
        if (!load.issuing) {
            return;
        }
        SimulatedCluster c = load.cluster;
        List<Bytes> keys = load.keys;
        History history = load.history;
        Bytes key = keys.get(w.random.nextInt(keys.size()));
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
            // Expect what we last saw (often still right, so some CASes succeed), else "absent".
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
                // Think a little, then go again.
                c.sim.schedule(Duration.ofMillis(w.random.nextInt(21)), () -> next(load, w));
            } else if (load.replacements < MAX_REPLACEMENTS) {
                // Outcome unknown: retire this client (it may still have a request in flight)
                // and bring in a fresh one with a new session.
                load.replacements++;
                load.spawn();
            }
        });
    }
}
