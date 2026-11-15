package dev.raftkv.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import dev.raftkv.client.cli.RaftKVCli;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVResult;
import dev.raftkv.linearizability.History;
import dev.raftkv.linearizability.LinearizabilityChecker;
import dev.raftkv.linearizability.Operation;
import dev.raftkv.shard.ShardConfig;
import dev.raftkv.shard.ShardedKVClient;

/**
 * Jepsen-lite (SPEC §9.7): real server processes, real TCP, a load generator recording a history
 * with wall-clock times, and a nemesis that kill -9s and restarts servers and moves shards
 * between groups, all at once. The history then goes to the same linearizability checker as the
 * simulations. Unlike the simulations this is not reproducible from a seed (real time, real OS);
 * the seed only drives the random choices.
 *
 * <pre>
 *   .\mvnw.cmd -q test-compile
 *   java -cp "target\classes;target\test-classes" dev.raftkv.server.JepsenLite [seconds] [seed]
 * </pre>
 */
public final class JepsenLite {

    /** Outcome of one run. */
    public record Report(boolean linearizable, int operations, int unknown, int kills, int moves, String details) {
    }

    private JepsenLite() {
    }

    public static void main(String[] args) {
        int seconds = args.length > 0 ? Integer.parseInt(args[0]) : 60;
        long seed = args.length > 1 ? Long.parseLong(args[1]) : System.nanoTime();
        Report r = run(Duration.ofSeconds(seconds), seed);
        System.out.println("[jepsen-lite] seed " + seed + ": " + r.operations() + " operations (" + r.unknown()
                + " with unknown outcome), " + r.kills() + " kill -9s, " + r.moves() + " shard moves: "
                + (r.linearizable() ? "LINEARIZABLE" : "NOT LINEARIZABLE\n" + r.details()));
        System.exit(r.linearizable() ? 0 : 1);
    }

    public static Report run(Duration duration, long seed) {
        Random random = new Random(seed);
        History history = new History(System::nanoTime);
        List<Bytes> keys = List.of(Bytes.ofUtf8("a"), Bytes.ofUtf8("b"), Bytes.ofUtf8("c"));
        int kills = 0;
        int moves = 0;
        try (ProcessCluster cluster = new ProcessCluster(3)) {
            cluster.startAll();
            try (RaftKVCli cli = new RaftKVCli(cluster.addresses(), Duration.ofSeconds(8))) {
                ShardedKVClient client = cli.client();
                client.put("warm", "up"); // waits for the cluster's first configuration
                int workers = 4;
                long end = System.nanoTime() + duration.toNanos();
                List<Thread> threads = new ArrayList<>();
                for (int w = 0; w < workers; w++) {
                    int id = w;
                    Random wr = new Random(seed * 31 + w);
                    Thread t = new Thread(() -> work(id, wr, client, keys, history, end), "jepsen-worker-" + w);
                    t.start();
                    threads.add(t);
                }
                // The nemesis: every 1-3 s, kill a server, bring a dead one back, or move a shard.
                while (System.nanoTime() < end) {
                    sleep(1000 + random.nextInt(2000));
                    List<NodeId> up = cluster.ids().stream().filter(cluster::isRunning).toList();
                    List<NodeId> down = cluster.ids().stream().filter(id -> !cluster.isRunning(id)).toList();
                    int pick = random.nextInt(3);
                    if (!down.isEmpty() && (pick == 0 || up.size() < 3)) {
                        cluster.start(down.get(random.nextInt(down.size())));
                    } else if (pick == 1 && up.size() == 3) {
                        cluster.kill(up.get(random.nextInt(up.size())));
                        kills++;
                    } else {
                        try {
                            ShardConfig cfg = client.controller().query(-1).get(3, TimeUnit.SECONDS);
                            List<Integer> gids = new ArrayList<>(cfg.gids());
                            if (!gids.isEmpty()) {
                                client.controller().move(random.nextInt(cfg.numShards()), gids.get(random.nextInt(gids.size())))
                                        .get(5, TimeUnit.SECONDS);
                                moves++;
                            }
                        } catch (Exception e) {
                            // the controller may be briefly unavailable; the nemesis just tries again later
                        }
                    }
                }
                cluster.startAll(); // heal, so every in-flight operation can finish
                for (Thread t : threads) {
                    t.join(TimeUnit.SECONDS.toMillis(30));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Report(false, 0, 0, kills, moves, "interrupted");
        }
        List<Operation> ops;
        synchronized (history) {
            ops = history.operations();
        }
        int unknown = (int) ops.stream().filter(Operation::isUnknown).count();
        LinearizabilityChecker.Result check = LinearizabilityChecker.check(ops);
        return new Report(check.linearizable(), ops.size(), unknown, kills, moves, check.report());
    }

    /** One logical client: sequential random operations with unique values until the deadline. */
    private static void work(int id, Random r, ShardedKVClient client, List<Bytes> keys, History history, long end) {
        int counter = 0;
        while (System.nanoTime() < end) {
            Bytes key = keys.get(r.nextInt(keys.size()));
            Bytes value = Bytes.ofUtf8("w" + id + "-" + (++counter));
            int kind = r.nextInt(10);
            int handle;
            CompletableFuture<KVResult> op;
            synchronized (history) {
                if (kind < 4) {
                    handle = history.invoke(id, Operation.Kind.PUT, key, Optional.of(value), Optional.empty());
                    op = client.putAsync(key, value).thenApply(KVResult.PutResult::new);
                } else if (kind < 8) {
                    handle = history.invoke(id, Operation.Kind.GET, key, Optional.empty(), Optional.empty());
                    op = client.getAsync(key).thenApply(KVResult.GetResult::new);
                } else {
                    Optional<Bytes> expected = r.nextBoolean() ? Optional.empty() : Optional.of(Bytes.ofUtf8("w" + id + "-" + (counter - 1)));
                    handle = history.invoke(id, Operation.Kind.CAS, key, Optional.of(value), expected);
                    op = client.casAsync(key, expected, value).thenApply(c -> c);
                }
            }
            try {
                KVResult result = op.get(20, TimeUnit.SECONDS);
                synchronized (history) {
                    history.ok(handle, result);
                }
            } catch (Exception e) {
                // Timed out or failed: the outcome is unknown, and this logical client stops (a new
                // operation from it could otherwise overlap the unknown one, which a real client
                // never does).
                return;
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
