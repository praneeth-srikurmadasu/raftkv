package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVResult;
import dev.raftkv.linearizability.History;
import dev.raftkv.linearizability.LinearizabilityChecker;
import dev.raftkv.linearizability.Operation;

/**
 * Milestone 11 bug detection (SPEC §9.6) for the read paths. Each bug only matters in a narrow
 * window (a deposed leader still answering), which random chaos rarely hits because check-quorum
 * and stickiness close most of it. So each is driven into that window by a scripted partition,
 * and the linearizability checker is the judge: the buggy code must produce a history it rejects,
 * the correct code must never serve the read at all.
 */
class ReadBugDetectionTest {

    private static final ClientConfig CLIENT = new ClientConfig(Duration.ofMillis(200), Duration.ofMillis(5),
            Duration.ofMillis(100), Duration.ofSeconds(20));
    private static final Bytes X = Bytes.ofUtf8("x");
    private static final int SEEDS = 20;

    /** The deposed leader is cut off; without check-quorum it stays "leader" and keeps answering. */
    @Test
    void detectsReadIndexWithoutAHeartbeatRound() {
        RaftConfig base = RaftConfig.defaults().withReadMode(ReadMode.READ_INDEX).withElectionExtensions(true, false);
        assertThat(firstStaleRead(base.withBugs(Bug.READINDEX_WITHOUT_HEARTBEAT), ReadBugDetectionTest::isolatedLeader))
                .as("bug caught").isPresent();
        assertThat(firstStaleRead(base, ReadBugDetectionTest::isolatedLeader)).as("correct code").isEmpty();
    }

    /**
     * Without stickiness a follower that still hears from the leader votes for a node the leader
     * can't reach, while the leader's lease is valid. The old leader then answers lease reads for
     * a value the new leader has already overwritten.
     */
    @Test
    void detectsLeasesWithoutLeaderStickiness() {
        RaftConfig base = RaftConfig.defaults().withReadMode(ReadMode.LEASE);
        assertThat(firstStaleRead(base.withBugs(Bug.LEASE_WITHOUT_STICKINESS), ReadBugDetectionTest::rivalBehindTheLeadersBack))
                .as("bug caught").isPresent();
        assertThat(firstStaleRead(base, ReadBugDetectionTest::rivalBehindTheLeadersBack)).as("correct code").isEmpty();
    }

    /** Sets up the partition; returns false if no other leader could be elected (nothing to read against). */
    @FunctionalInterface
    private interface Scenario {
        boolean deposeBehindItsBack(SimulatedCluster c, RaftNode leader, List<NodeId> others);
    }

    private static boolean isolatedLeader(SimulatedCluster c, RaftNode leader, List<NodeId> others) {
        c.net.isolate(leader.id());
        c.awaitLeader(others, Duration.ofSeconds(3));
        return true;
    }

    private static boolean rivalBehindTheLeadersBack(SimulatedCluster c, RaftNode leader, List<NodeId> others) {
        NodeId rival = others.get(1);
        NodeId follower = others.get(0);
        c.net.blockOneWay(leader.id(), rival);
        c.net.blockOneWay(rival, leader.id());
        boolean campaigning = c.sim.runUntil(() -> c.node(rival).role() == Role.CANDIDATE, Duration.ofSeconds(2));
        if (!campaigning) {
            return false; // the follower kept refusing the rival: stickiness works
        }
        c.net.blockOneWay(follower, leader.id()); // the old leader never hears about the new term
        return c.sim.runUntil(() -> c.node(rival).role() == Role.LEADER, Duration.ofSeconds(1));
    }

    private static Optional<Long> firstStaleRead(RaftConfig config, Scenario scenario) {
        for (long seed = 1; seed <= SEEDS; seed++) {
            if (staleReadServed(seed, config, scenario)) {
                System.out.println("[bug-detection] " + config.bugs() + " (" + config.readMode() + "): caught at seed " + seed);
                return Optional.of(seed);
            }
        }
        return Optional.empty();
    }

    /** True if the old leader served a read that makes the history non-linearizable. */
    private static boolean staleReadServed(long seed, RaftConfig config, Scenario scenario) {
        SimulatedCluster c = SimulatedCluster.kv(seed, 3, config, SimulatedCluster.Storage.MEMORY);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        List<NodeId> others = c.ids.stream().filter(id -> !id.equals(leader.id())).toList();
        History h = new History(c.sim::now);
        RaftKVClient client = c.newClient(CLIENT);
        int w1 = h.invoke(1, Operation.Kind.PUT, X, Optional.of(Bytes.ofUtf8("1")), Optional.empty());
        h.ok(w1, new KVResult.PutResult(c.await(client.putAsync(X, Bytes.ofUtf8("1")), Duration.ofSeconds(5))));
        c.sim.runUntil(() -> config.readMode() != ReadMode.LEASE || leader.holdsLease(), Duration.ofSeconds(1));

        if (!scenario.deposeBehindItsBack(c, leader, others)) {
            return false;
        }
        RaftKVClient other = c.newClient(CLIENT, others.toArray(NodeId[]::new));
        int w2 = h.invoke(2, Operation.Kind.PUT, X, Optional.of(Bytes.ofUtf8("2")), Optional.empty());
        h.ok(w2, new KVResult.PutResult(c.await(other.putAsync(X, Bytes.ofUtf8("2")), Duration.ofSeconds(5))));

        c.run(Duration.ofMillis(1)); // strictly after the write returned
        int r = h.invoke(3, Operation.Kind.GET, X, Optional.empty(), Optional.empty());
        CompletableFuture<Long> read = leader.read(config.readMode());
        c.run(Duration.ofMillis(2));
        if (!read.isDone() || read.isCompletedExceptionally()) {
            return false;
        }
        h.ok(r, new KVResult.GetResult(c.kvStore(leader.id()).peek(X)));
        return !LinearizabilityChecker.check(h.operations()).linearizable();
    }
}
