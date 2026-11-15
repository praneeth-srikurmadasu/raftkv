package dev.raftkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.sim.SimulationException;

/**
 * Chaos for the KV layer: the replication nemesis, plus three clients each writing their own
 * key over and over (v1, v2, ...) through the real client library while the network drops,
 * duplicates and reorders messages and nodes crash.
 *
 * <p>Because each client waits for one PUT before sending the next, every PUT must report the
 * previous value as the client's own previous write: a retry that executed twice, or a lost
 * acknowledged write, breaks that chain. After recovery, every replica must hold the same state
 * (digest) and have executed each client operation exactly once.
 */
final class KVChaos {

    private KVChaos() {
    }

    record Result(long seed, Optional<String> failure, long operations, long retries) {
        boolean passed() {
            return failure.isEmpty();
        }
    }

    private static final class Writer {
        final RaftKVClient client;
        final Bytes key;
        int written;
        Optional<Bytes> lastValue = Optional.empty();
        CompletableFuture<Optional<Bytes>> inFlight;
        String failure;

        Writer(RaftKVClient client, Bytes key) {
            this.client = client;
            this.key = key;
        }
    }

    static Result run(long seed, RaftConfig config, SimulatedCluster.Storage storage) {
        Random r = new Random(seed);
        SimulatedCluster c = SimulatedCluster.kv(seed, r.nextBoolean() ? 3 : 5, SnapshotSwarm.pick(seed, config), storage);
        List<Writer> writers = new ArrayList<>();
        try {
            boolean partitions = r.nextInt(4) != 0;
            boolean crashes = r.nextInt(4) != 0;
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(5 + r.nextInt(26)));
            c.net.setDropRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.15);
            c.net.setDuplicateRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.15);
            c.startAll();
            ClientConfig cfg = new ClientConfig(Duration.ofMillis(50 + r.nextInt(200)), Duration.ofMillis(5),
                    Duration.ofMillis(300), Duration.ofSeconds(120));
            for (int i = 0; i < 3; i++) {
                writers.add(new Writer(c.newClient(cfg), Bytes.ofUtf8("client-" + i)));
            }

            Random nemesis = c.sim.forkRandom();
            long chaosEnd = Duration.ofSeconds(10).toNanos();
            while (c.sim.now() < chaosEnd) {
                long until = Math.min(chaosEnd, c.sim.now() + Duration.ofMillis(100 + nemesis.nextInt(501)).toNanos());
                while (c.sim.now() < until) {
                    pump(writers);
                    c.sim.runUntil(() -> writers.stream().anyMatch(w -> w.inFlight.isDone()),
                            Duration.ofNanos(until - c.sim.now()));
                }
                ElectionChaos.act(c, nemesis, partitions, crashes);
            }
            // Recovery: heal, restart, and let every writer finish its current operation.
            c.net.heal();
            c.crashedNodes().forEach(c::restart);
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            for (Writer w : writers) {
                c.await(w.inFlight, Duration.ofSeconds(120));
            }
            pumpCompletions(writers);
            for (Writer w : writers) {
                if (w.failure != null) {
                    throw new AssertionError(w.failure);
                }
            }
            RaftNode leader = c.awaitLeader(Duration.ofSeconds(10));
            c.awaitIdenticalApplied(Duration.ofSeconds(60));
            long expectedOps = writers.stream().mapToLong(w -> w.written).sum();
            long digest = c.kvStore(leader.id()).digest();
            for (NodeId id : c.ids) {
                if (c.kvStore(id).executedCommands() != expectedOps) {
                    throw new AssertionError(id + " executed " + c.kvStore(id).executedCommands()
                            + " commands, clients completed " + expectedOps);
                }
                if (c.kvStore(id).digest() != digest) {
                    throw new AssertionError(id + " has a different KV state than the leader");
                }
            }
            for (Writer w : writers) {
                if (!c.kvStore(leader.id()).peek(w.key).equals(w.lastValue)) {
                    throw new AssertionError("final value of " + w.key.toUtf8() + " is not the last acknowledged write");
                }
            }
            long retries = writers.stream().mapToLong(w -> w.client.retries()).sum();
            return new Result(seed, Optional.empty(), expectedOps, retries);
        } catch (AssertionError | SimulationException | IllegalStateException e) {
            String message = e.getMessage() + (e.getCause() != null ? " / cause: " + e.getCause().getMessage() : "");
            return new Result(seed, Optional.of(message), 0, 0);
        }
    }

    /** Checks finished PUTs and starts the next one for every idle writer. */
    private static void pump(List<Writer> writers) {
        pumpCompletions(writers);
        for (Writer w : writers) {
            if (w.inFlight == null || w.inFlight.isDone()) {
                Bytes next = Bytes.ofUtf8("v" + (w.written + 1));
                w.inFlight = w.client.putAsync(w.key, next);
                Optional<Bytes> expectedPrevious = w.lastValue;
                w.inFlight.thenAccept(previous -> {
                    if (!previous.equals(expectedPrevious)) {
                        w.failure = "PUT " + next.toUtf8() + " on " + w.key.toUtf8() + " returned "
                                + previous.map(Bytes::toUtf8) + " instead of " + expectedPrevious.map(Bytes::toUtf8)
                                + " (an operation was applied twice or an acknowledged write was lost)";
                    }
                    w.written++;
                    w.lastValue = Optional.of(next);
                });
            }
        }
    }

    private static void pumpCompletions(List<Writer> writers) {
        for (Writer w : writers) {
            if (w.failure != null) {
                throw new AssertionError(w.failure);
            }
            if (w.inFlight != null && w.inFlight.isCompletedExceptionally()) {
                throw new AssertionError("an operation failed: " + w.inFlight.handle((v, e) -> e).join());
            }
        }
    }

}
