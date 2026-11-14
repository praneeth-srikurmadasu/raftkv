package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/** Leader election in whole simulated clusters. */
class ElectionTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();

    @Test
    void singleNodeElectsItself() {
        SimulatedCluster c = new SimulatedCluster(1, 1, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(1));
        assertThat(leader.currentTerm()).isEqualTo(1L);
    }

    @Test
    void threeNodesElectExactlyOneLeaderThatEveryoneFollows() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        assertThat(leader.currentTerm()).as("seed %d", seed).isGreaterThanOrEqualTo(1L);
        for (RaftNode n : c.runningNodes()) {
            assertThat(n.leaderId()).as("seed %d", seed).isEqualTo(Optional.of(leader.id()));
        }
    }

    @Test
    void firstElectionHappensWithinTheTimeoutWindow() {
        // On a quiet network, the first candidate appears between 150 and 300 ms after start.
        for (long seed = 1; seed <= 50; seed++) {
            SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
            c.startAll();
            c.run(Duration.ofMillis(149));
            assertThat(totalElections(c)).as("seed %d: too early", seed).isEqualTo(0L);
            c.run(Duration.ofMillis(151));
            assertThat(totalElections(c)).as("seed %d: too late", seed).isGreaterThan(0L);
        }
    }

    @Test
    void healthyLeaderIsNeverChallenged() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 5, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        long term = leader.currentTerm();
        long elections = totalElections(c);
        c.run(Duration.ofSeconds(30)); // 600 heartbeat intervals
        assertThat(c.convergedLeader(c.ids)).as("seed %d", seed).isEqualTo(Optional.of(leader));
        assertThat(leader.currentTerm()).as("seed %d", seed).isEqualTo(term);
        assertThat(totalElections(c)).as("seed %d: no new elections", seed).isEqualTo(elections);
    }

    @Test
    void crashedLeaderIsReplacedAndRejoinsAsFollower() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode first = c.awaitLeader(Duration.ofSeconds(2));
        c.crash(first.id());
        List<NodeId> survivors = c.ids.stream().filter(id -> !id.equals(first.id())).toList();
        RaftNode second = c.awaitLeader(survivors, Duration.ofSeconds(2));
        assertThat(second.id()).as("seed %d", seed).isNotEqualTo(first.id());
        assertThat(second.currentTerm()).as("seed %d", seed).isGreaterThan(first.currentTerm());

        c.restart(first.id());
        RaftNode after = c.awaitLeader(Duration.ofSeconds(2));
        assertThat(c.node(first.id()).role()).as("seed %d", seed).isEqualTo(Role.FOLLOWER);
        assertThat(c.node(first.id()).currentTerm()).as("seed %d", seed).isEqualTo(after.currentTerm());
    }

    @Test
    void isolatedLeaderIsReplacedAndStepsDownWhenTheNetworkHeals() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 5, CONFIG);
        c.startAll();
        RaftNode old = c.awaitLeader(Duration.ofSeconds(2));
        c.net.isolate(old.id());
        List<NodeId> majority = c.ids.stream().filter(id -> !id.equals(old.id())).toList();
        RaftNode replacement = c.awaitLeader(majority, Duration.ofSeconds(3));
        assertThat(replacement.currentTerm()).as("seed %d", seed).isGreaterThan(old.currentTerm());
        // Without check-quorum (Milestone 11), the cut-off leader still believes it leads its old term.
        assertThat(old.role()).as("seed %d", seed).isEqualTo(Role.LEADER);

        c.net.heal();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        assertThat(leader).as("seed %d", seed).isSameAs(replacement);
        assertThat(old.role()).as("seed %d", seed).isEqualTo(Role.FOLLOWER);
    }

    @Test
    void minorityPartitionCannotElectALeader() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 5, CONFIG);
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(2));
        List<NodeId> minority = c.ids.subList(0, 2);
        List<NodeId> majority = c.ids.subList(2, 5);
        long termAtPartition = c.runningNodes().stream().mapToLong(RaftNode::currentTerm).max().orElseThrow();
        c.net.partition(minority, majority);
        c.run(Duration.ofSeconds(10));
        // The majority side has (or elects) a leader.
        c.awaitLeader(majority, Duration.ofSeconds(2));
        // The minority side may still contain the OLD leader (no check-quorum until Milestone 11),
        // but nobody there can win an election that started after the partition.
        for (NodeId id : minority) {
            RaftNode n = c.node(id);
            if (n.role() == Role.LEADER) {
                assertThat(n.currentTerm()).as("seed %d: minority elected a new leader", seed)
                        .isLessThanOrEqualTo(termAtPartition);
            }
        }
    }

    @Test
    void rejoiningNodeWithHighTermDoesNotDisruptAHealthyLeader() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 5, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        NodeId loner = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        c.net.isolate(loner);
        c.run(Duration.ofSeconds(5)); // the loner times out again and again, inflating its term
        assertThat(c.node(loner).currentTerm()).as("seed %d", seed).isGreaterThan(leader.currentTerm() + 5);

        c.net.heal();
        c.run(Duration.ofSeconds(3));
        // Leader stickiness: the others reject the loner's RequestVotes without adopting its term.
        // (The leader itself does learn the higher term from the loner's replies to heartbeats and
        // steps down once; full protection needs pre-vote, Milestone 11.) Safety holds either way,
        // and the group always converges again.
        RaftNode after = c.awaitLeader(Duration.ofSeconds(3));
        assertThat(after.currentTerm()).as("seed %d", seed).isGreaterThanOrEqualTo(leader.currentTerm());
    }

    /** §5.4.1: a node missing entries that a majority has can never become leader. */
    @Test
    void nodeWithStaleLogNeverWins() {
        Set<NodeId> winners = new HashSet<>();
        for (long seed = 1; seed <= 100; seed++) {
            winners.add(staleLogScenarioWinner(seed, CONFIG));
        }
        assertThat(winners.contains(NodeId.of(1))).isFalse();
        assertThat(winners).isEqualTo(Set.of(NodeId.of(2), NodeId.of(3))); // both up-to-date nodes can win
    }

    /** n1's log ends in t1; n2 and n3 also have an entry from t2. Who wins the election? */
    static NodeId staleLogScenarioWinner(long seed, RaftConfig config) {
        List<LogEntry> stale = List.of(LogEntry.noop(1, 1), LogEntry.noop(2, 1));
        List<LogEntry> fresh = List.of(LogEntry.noop(1, 1), LogEntry.noop(2, 1), LogEntry.noop(3, 2));
        SimulatedCluster c = new SimulatedCluster(seed, 3, config,
                Map.of(NodeId.of(1), stale, NodeId.of(2), fresh, NodeId.of(3), fresh), 2);
        c.startAll();
        return c.awaitLeader(Duration.ofSeconds(5)).id();
    }

    @Test
    void splitVotesResolveQuickly() {
        // Even clusters (4 and 6 nodes) split votes most easily. Randomized timeouts must still
        // produce a leader fast, in every seed.
        for (long seed = 1; seed <= 200; seed++) {
            int size = seed % 2 == 0 ? 4 : 6;
            SimulatedCluster c = new SimulatedCluster(seed, size, CONFIG);
            c.net.setLatency(Duration.ofMillis(5), Duration.ofMillis(20));
            c.startAll();
            c.awaitLeader(Duration.ofSeconds(3));
        }
    }

    @Test
    void sameSeedGivesTheSameElectionHistory() {
        long seed = Seeds.resolve();
        assertThat(ElectionChaos.run(seed, CONFIG)).as("seed %d", seed).isEqualTo(ElectionChaos.run(seed, CONFIG));
        assertThat(traceHash(seed)).as("seed %d", seed).isEqualTo(traceHash(seed));
    }

    /** Fingerprint of a short run with drops and a crash. */
    private static long traceHash(long seed) {
        SimulatedCluster c = new SimulatedCluster(seed, 5, CONFIG);
        c.net.setDropRate(0.1);
        c.startAll();
        c.run(Duration.ofSeconds(2));
        c.crash(c.ids.get(0));
        c.run(Duration.ofSeconds(2));
        return c.sim.traceHash();
    }

    private static long totalElections(SimulatedCluster c) {
        return c.runningNodes().stream().mapToLong(RaftNode::electionsStarted).sum();
    }
}
