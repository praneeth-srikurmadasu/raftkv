package dev.raftkv.server;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.ReadMode;
import dev.raftkv.shard.ShardedKVClient;

/**
 * SPEC §10 benchmarks, on real TCP (loopback) and real disks, all nodes in one JVM: write
 * throughput with and without pipelining, the three read modes, failover time with and without
 * pre-vote, catch-up time with and without snapshots, and scale-out over data groups.
 *
 * <pre>
 *   .\mvnw.cmd -q test-compile
 *   java -cp "target\classes;target\test-classes" dev.raftkv.server.Bench [results.md]
 * </pre>
 *
 * Numbers depend entirely on the machine (CPU, and above all how fast the disk does fsync); the
 * report starts with what it ran on. Everything shares one machine, so this measures the code, not
 * a production deployment.
 */
public final class Bench {

    private static final int CLIENTS = 32;
    private static final Duration WARMUP = Duration.ofSeconds(1);
    private static final Duration MEASURE = Duration.ofSeconds(5);

    private final StringBuilder report = new StringBuilder();

    public static void main(String[] args) throws Exception {
        Bench b = new Bench();
        b.run();
        System.out.println(b.report);
        if (args.length > 0) {
            Files.writeString(Path.of(args[0]), b.report.toString(), StandardCharsets.UTF_8);
        }
        System.exit(0);
    }

    private void run() throws Exception {
        line("# RaftKV benchmark results");
        line("");
        line("- Machine: " + Runtime.getRuntime().availableProcessors() + " CPUs, " + System.getProperty("os.name") + " "
                + System.getProperty("os.arch") + ", Java " + Runtime.version() + ", max heap "
                + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB");
        line("- Setup: 3 server nodes in one JVM, real TCP over loopback, real files with fsync; "
                + CLIENTS + " concurrent closed-loop clients; " + WARMUP.toSeconds() + " s warm-up, "
                + MEASURE.toSeconds() + " s measured per row.");
        line("");

        line("## 1. Writes: pipelining");
        line("");
        line("| AppendEntries in flight per follower | ops/s | p50 ms | p99 ms |");
        line("|---|---|---|---|");
        for (int window : new int[] {1, 8}) {
            Result r = load(RaftConfig.defaults().withMaxInflightAppends(window), 1, false);
            line("| " + window + (window == 1 ? " (stop-and-wait)" : "") + " | " + r.row() + " |");
        }
        line("");

        line("## 2. Reads: log vs ReadIndex vs lease");
        line("");
        line("| read mode | ops/s | p50 ms | p99 ms |");
        line("|---|---|---|---|");
        for (ReadMode mode : ReadMode.values()) {
            Result r = load(RaftConfig.defaults().withReadMode(mode), 1, true);
            line("| " + mode + " | " + r.row() + " |");
        }
        line("");

        line("## 3. Scale-out over data groups (one machine: shows per-group overhead, not real scaling)");
        line("");
        line("| data groups | ops/s (writes) | p50 ms | p99 ms |");
        line("|---|---|---|---|");
        for (int groups : new int[] {1, 2, 4}) {
            Result r = load(RaftConfig.defaults(), groups, false);
            line("| " + groups + " | " + r.row() + " |");
        }
        line("");

        line("## 4. Failover: leader stopped -> first successful write (10 trials)");
        line("");
        line("| pre-vote | min ms | median ms | max ms |");
        line("|---|---|---|---|");
        for (boolean preVote : new boolean[] {true, false}) {
            List<Long> t = failover(RaftConfig.defaults().withElectionExtensions(preVote, true), 10);
            Collections.sort(t);
            line("| " + (preVote ? "on" : "off") + " | " + t.get(0) + " | " + t.get(t.size() / 2) + " | " + t.get(t.size() - 1) + " |");
        }
        line("");

        line("## 5. Catch-up of a follower that missed 3,000 writes");
        line("");
        line("| snapshots | catch-up ms |");
        line("|---|---|");
        line("| off | " + catchUp(RaftConfig.defaults().withSnapshots(0, 64 * 1024)) + " |");
        line("| every 500 entries (follower gets a snapshot) | " + catchUp(RaftConfig.defaults().withSnapshots(500, 64 * 1024)) + " |");
    }

    // --------------------------------------------------------------------------- measurements

    private record Result(double opsPerSecond, double p50, double p99) {
        String row() {
            return String.format(Locale.ROOT, "%,.0f | %.1f | %.1f", opsPerSecond, p50, p99);
        }
    }

    /** CLIENTS closed loops of PUT (or GET on preloaded keys) for MEASURE after WARMUP. */
    private static Result load(RaftConfig config, int groups, boolean reads) throws InterruptedException {
        try (LocalCluster cluster = new LocalCluster(3, groups, config)) {
            for (int g = 1; g <= groups; g++) {
                cluster.awaitLeader(GroupId.of(g));
            }
            ShardedKVClient client = cluster.newClient(Duration.ofSeconds(30));
            for (int i = 0; i < 100; i++) {
                client.put("key" + i, "value" + i);
            }
            AtomicBoolean measuring = new AtomicBoolean();
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong done = new AtomicLong();
            ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
            for (int c = 0; c < CLIENTS; c++) {
                loop(client, c, reads, running, measuring, done, latencies);
            }
            Thread.sleep(WARMUP.toMillis());
            measuring.set(true);
            long start = System.nanoTime();
            Thread.sleep(MEASURE.toMillis());
            measuring.set(false);
            long elapsed = System.nanoTime() - start;
            running.set(false);
            Thread.sleep(300);
            List<Long> sorted = new ArrayList<>(latencies);
            Collections.sort(sorted);
            return new Result(done.get() * 1e9 / elapsed, percentile(sorted, 0.50), percentile(sorted, 0.99));
        }
    }

    private static void loop(ShardedKVClient client, int id, boolean reads, AtomicBoolean running, AtomicBoolean measuring,
                             AtomicLong done, ConcurrentLinkedQueue<Long> latencies) {
        if (!running.get()) {
            return;
        }
        long t0 = System.nanoTime();
        Bytes key = Bytes.ofUtf8("key" + (int) (Math.random() * 100));
        var op = reads ? client.getAsync(key).thenApply(v -> null)
                : client.putAsync(key, Bytes.ofUtf8("v" + id + "-" + t0)).thenApply(v -> null);
        op.whenComplete((v, e) -> {
            if (e == null && measuring.get()) {
                done.incrementAndGet();
                latencies.add(System.nanoTime() - t0);
            }
            loop(client, id, reads, running, measuring, done, latencies);
        });
    }

    private static List<Long> failover(RaftConfig config, int trials) {
        List<Long> times = new ArrayList<>();
        try (LocalCluster cluster = new LocalCluster(3, 1, config)) {
            ShardedKVClient client = cluster.newClient(Duration.ofSeconds(30));
            client.put("warm", "up");
            for (int t = 0; t < trials; t++) {
                NodeId leader = cluster.awaitLeader(GroupId.of(1));
                client.put("before" + t, "x"); // the client knows the current leader
                long start = System.nanoTime();
                cluster.stop(leader);
                client.put("after" + t, "y");
                times.add((System.nanoTime() - start) / 1_000_000);
                cluster.start(leader);
                cluster.awaitConverged();
            }
        }
        return times;
    }

    private static long catchUp(RaftConfig config) {
        try (LocalCluster cluster = new LocalCluster(3, 1, config)) {
            ShardedKVClient client = cluster.newClient(Duration.ofSeconds(60));
            client.put("warm", "up");
            NodeId leader = cluster.awaitLeader(GroupId.of(1));
            NodeId follower = cluster.ids().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
            cluster.stop(follower);
            List<java.util.concurrent.CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < 3000; i++) {
                writes.add(client.putAsync(Bytes.ofUtf8("k" + (i % 500)), Bytes.ofUtf8("v" + i)));
                if (writes.size() >= 64) {
                    writes.remove(0).join();
                }
            }
            writes.forEach(java.util.concurrent.CompletableFuture::join);
            long target = cluster.status(leader, GroupId.of(1)).commitIndex();
            long start = System.nanoTime();
            cluster.start(follower);
            RealNet.await("follower caught up", 120_000, () -> cluster.status(follower, GroupId.of(1)).lastApplied() >= target);
            return (System.nanoTime() - start) / 1_000_000;
        }
    }

    private static double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) {
            return Double.NaN;
        }
        return sorted.get(Math.min(sorted.size() - 1, (int) (p * sorted.size()))) / 1e6;
    }

    private void line(String s) {
        report.append(s).append('\n');
        System.err.println(s);
    }
}
