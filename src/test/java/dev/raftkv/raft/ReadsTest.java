package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVResult;
import dev.raftkv.linearizability.History;
import dev.raftkv.linearizability.LinearizabilityChecker;
import dev.raftkv.linearizability.Operation;
import dev.raftkv.protocol.AppendEntries;
import dev.raftkv.sim.Seeds;

/** Milestone 11: linearizable reads without log entries (SPEC §4.4, thesis §6.4). */
class ReadsTest {

    private static final ClientConfig CLIENT = new ClientConfig(Duration.ofMillis(200), Duration.ofMillis(5),
            Duration.ofMillis(100), Duration.ofSeconds(20));

    private static RaftConfig config(ReadMode mode) {
        return RaftConfig.defaults().withReadMode(mode);
    }

    @Test
    void readIndexGetsAddNothingToTheLog() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.kv(seed, 3, config(ReadMode.READ_INDEX), SimulatedCluster.Storage.MEMORY);
        c.startAll();
        RaftKVClient client = c.newClient(CLIENT);
        c.await(client.putAsync(Bytes.ofUtf8("x"), Bytes.ofUtf8("1")), Duration.ofSeconds(5));
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        c.awaitAppliedEverywhere(leader.log().lastIndex(), Duration.ofSeconds(5));
        long before = leader.log().lastIndex();
        for (int i = 0; i < 20; i++) {
            assertThat(c.await(client.getAsync(Bytes.ofUtf8("x")), Duration.ofSeconds(5)))
                    .isEqualTo(Optional.of(Bytes.ofUtf8("1")));
        }
        assertThat(leader.log().lastIndex()).as("seed %d", seed).isEqualTo(before);
        // And LOG mode, for contrast, does write every GET into the log.
        SimulatedCluster logMode = SimulatedCluster.kv(seed, 3, config(ReadMode.LOG), SimulatedCluster.Storage.MEMORY);
        logMode.startAll();
        RaftKVClient c2 = logMode.newClient(CLIENT);
        logMode.await(c2.putAsync(Bytes.ofUtf8("x"), Bytes.ofUtf8("1")), Duration.ofSeconds(5));
        RaftNode l2 = logMode.awaitLeader(Duration.ofSeconds(2));
        long before2 = l2.log().lastIndex();
        for (int i = 0; i < 20; i++) {
            logMode.await(c2.getAsync(Bytes.ofUtf8("x")), Duration.ofSeconds(5));
        }
        assertThat(l2.log().lastIndex()).as("seed %d", seed).isGreaterThanOrEqualTo(before2 + 20);
    }

    @Test
    void manyConcurrentReadIndexReadsShareOneHeartbeatRound() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, config(ReadMode.READ_INDEX));
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        c.sim.runUntil(() -> leader.commitIndex() >= 1, Duration.ofSeconds(2));
        c.run(Duration.ofMillis(10));
        AtomicLong appends = new AtomicLong();
        c.deliveryObserver = e -> {
            if (e.message() instanceof AppendEntries) {
                appends.incrementAndGet();
            }
        };
        List<CompletableFuture<Long>> reads = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            reads.add(leader.read(ReadMode.READ_INDEX));
        }
        c.sim.runUntil(() -> reads.stream().allMatch(CompletableFuture::isDone), Duration.ofSeconds(2));
        assertThat(reads.stream().allMatch(f -> f.isDone() && !f.isCompletedExceptionally())).as("seed %d", seed).isTrue();
        // One round to each of 2 followers confirms all 100 (allow a periodic heartbeat that happened to fall in between).
        assertThat(appends.get()).as("seed %d", seed).isLessThanOrEqualTo(6L);
    }

    @Test
    void leaseReadsNeedNoRoundTripWhileTheLeaseHolds() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, config(ReadMode.LEASE));
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        c.sim.runUntil(leader::holdsLease, Duration.ofSeconds(2));
        assertThat(leader.holdsLease()).as("seed %d", seed).isTrue();
        long sent = c.net.sentCount();
        CompletableFuture<Long> read = leader.read(ReadMode.LEASE);
        assertThat(read.isDone()).as("answered at once, on the spot").isTrue();
        assertThat(c.net.sentCount()).isEqualTo(sent);
    }

    @Test
    void aLeaderCutOffFromItsGroupCannotServeReadIndexReads() {
        for (long seed = 1; seed <= 20; seed++) {
            for (ReadMode mode : List.of(ReadMode.READ_INDEX, ReadMode.LEASE)) {
                SimulatedCluster c = new SimulatedCluster(seed, 5, config(mode));
                c.startAll();
                RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
                c.sim.runUntil(() -> leader.commitIndex() >= 1, Duration.ofSeconds(2));
                c.net.isolate(leader.id());
                c.run(Duration.ofMillis(300)); // longer than any lease
                CompletableFuture<Long> read = leader.read(mode);
                c.run(Duration.ofSeconds(1));
                assertThat(read.isDone() && !read.isCompletedExceptionally())
                        .as("seed %d %s: an isolated leader answered a read", seed, mode).isFalse();
            }
        }
    }

    /**
     * SPEC §4.4: leases are only as safe as the clocks. Here the leader's clock runs at a tenth of
     * real speed, far beyond the assumed 10% drift: after it is cut off, it still believes its lease
     * holds while the others elect a new leader and accept a write. The stale read it then serves
     * makes the history non-linearizable, and the checker must say so. With an accurate clock the
     * same scenario serves no read at all.
     */
    @Test
    void aClockSkewedBeyondTheBoundBreaksLeasesAndTheCheckerNoticesIt() {
        assertThat(leaseScenario(0.1)).as("slow clock: stale read detected").isEqualTo(Optional.of(false));
        assertThat(leaseScenario(1.0)).as("accurate clock: no read served").isEqualTo(Optional.empty());
    }

    /** Linearizable verdict of the history, or empty if the cut-off leader served no read. */
    private static Optional<Boolean> leaseScenario(double leaderClockRate) {
        for (long seed = 1; seed <= 200; seed++) {
            SimulatedCluster c = SimulatedCluster.kv(seed, 3, config(ReadMode.LEASE), SimulatedCluster.Storage.MEMORY);
            c.setClockRate(NodeId.of(1), leaderClockRate);
            c.startAll();
            RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
            if (!leader.id().equals(NodeId.of(1))) {
                continue; // we need the skewed node to be the leader
            }
            Bytes x = Bytes.ofUtf8("x");
            History h = new History(c.sim::now);
            RaftKVClient client = c.newClient(CLIENT);
            int w1 = h.invoke(1, Operation.Kind.PUT, x, Optional.of(Bytes.ofUtf8("1")), Optional.empty());
            h.ok(w1, new KVResult.PutResult(c.await(client.putAsync(x, Bytes.ofUtf8("1")), Duration.ofSeconds(5))));
            c.sim.runUntil(leader::holdsLease, Duration.ofSeconds(1));

            c.net.isolate(leader.id());
            List<NodeId> others = List.of(NodeId.of(2), NodeId.of(3));
            c.awaitLeader(others, Duration.ofSeconds(3));
            RaftKVClient other = c.newClient(CLIENT, NodeId.of(2), NodeId.of(3));
            int w2 = h.invoke(2, Operation.Kind.PUT, x, Optional.of(Bytes.ofUtf8("2")), Optional.empty());
            h.ok(w2, new KVResult.PutResult(c.await(other.putAsync(x, Bytes.ofUtf8("2")), Duration.ofSeconds(5))));

            c.run(Duration.ofMillis(1)); // strictly after the write returned (equal times count as concurrent)
            int r = h.invoke(3, Operation.Kind.GET, x, Optional.empty(), Optional.empty());
            CompletableFuture<Long> read = leader.read(ReadMode.LEASE);
            c.run(Duration.ofMillis(5));
            if (!read.isDone() || read.isCompletedExceptionally()) {
                return Optional.empty();
            }
            h.ok(r, new KVResult.GetResult(c.kvStore(leader.id()).peek(x)));
            return Optional.of(LinearizabilityChecker.check(h.operations()).linearizable());
        }
        throw new AssertionError("node 1 never became leader");
    }
}
