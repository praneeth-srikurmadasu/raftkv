package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.AppendEntriesReply;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/** Log replication in whole simulated clusters. */
class ReplicationTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();

    private static List<String> appliedCommands(SimulatedCluster c, NodeId id) {
        return c.stateMachine(id).applied().stream().map(a -> a.command().toUtf8()).toList();
    }

    @Test
    void proposalsCommitInOrderAndEveryReplicaAppliesTheSameSequence() throws Exception {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(2));
        List<CompletableFuture<CommitResult>> futures = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            expected.add("cmd-" + i);
            futures.add(c.propose("cmd-" + i));
        }
        c.sim.runUntil(() -> futures.stream().allMatch(CompletableFuture::isDone), Duration.ofSeconds(5));
        long previous = 0;
        for (int i = 0; i < futures.size(); i++) {
            CommitResult r = futures.get(i).get();
            assertThat(r.index()).as("seed %d", seed).isGreaterThan(previous); // log order = proposal order
            assertThat(r.result()).isEqualTo(Bytes.ofUtf8("ok:cmd-" + i));
            previous = r.index();
        }
        c.awaitAppliedEverywhere(previous, Duration.ofSeconds(2));
        for (NodeId id : c.ids) {
            assertThat(appliedCommands(c, id)).as("seed %d, %s", seed, id).isEqualTo(expected);
        }
    }

    @Test
    void nonLeaderRejectsProposalsWithAHint() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        RaftNode follower = c.runningNodes().stream().filter(n -> n != leader).findFirst().orElseThrow();
        CompletableFuture<CommitResult> f = follower.propose(Bytes.ofUtf8("x"));
        assertThatThrownBy(f::get).isInstanceOf(ExecutionException.class);
        try {
            f.get();
        } catch (Exception e) {
            NotLeaderException nle = (NotLeaderException) e.getCause();
            assertThat(nle.leaderHint()).isEqualTo(java.util.Optional.of(leader.id()));
        }
    }

    @Test
    void crashedFollowerCatchesUpAfterRestart() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        NodeId down = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        c.crash(down);
        CompletableFuture<CommitResult> last = null;
        for (int i = 0; i < 300; i++) {      // more than one batch (64 entries) behind
            last = c.propose("w" + i);
        }
        CompletableFuture<CommitResult> lastWrite = last;
        c.sim.runUntil(lastWrite::isDone, Duration.ofSeconds(5));
        assertThat(lastWrite.isDone() && !lastWrite.isCompletedExceptionally()).as("seed %d: commits without it", seed).isTrue();

        c.restart(down);
        c.awaitAppliedEverywhere(lastWrite.join().index(), Duration.ofSeconds(5));
        assertThat(appliedCommands(c, down)).as("seed %d", seed).isEqualTo(appliedCommands(c, leader.id()));
        assertThat(appliedCommands(c, down)).hasSize(300);
    }

    @Test
    void committedEntriesSurviveLeaderChanges() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 5, CONFIG);
        c.startAll();
        List<String> expected = new ArrayList<>();
        for (int round = 0; round < 4; round++) {
            RaftNode leader = c.awaitLeader(c.ids.stream().filter(c::isRunning).toList(), Duration.ofSeconds(3));
            CompletableFuture<CommitResult> last = null;
            for (int i = 0; i < 20; i++) {
                String cmd = "r" + round + "-" + i;
                expected.add(cmd);
                last = leader.propose(Bytes.ofUtf8(cmd));
            }
            CompletableFuture<CommitResult> wait = last;
            c.sim.runUntil(wait::isDone, Duration.ofSeconds(3));
            assertThat(wait.isCompletedExceptionally()).as("seed %d", seed).isFalse();
            c.crash(leader.id());
            c.run(Duration.ofMillis(100));
            c.restart(leader.id());
        }
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(3));
        CompletableFuture<CommitResult> fin = leader.propose(Bytes.ofUtf8("final"));
        expected.add("final");
        c.sim.runUntil(fin::isDone, Duration.ofSeconds(3));
        c.awaitAppliedEverywhere(fin.join().index(), Duration.ofSeconds(3));
        for (NodeId id : c.ids) {
            assertThat(appliedCommands(c, id)).as("seed %d, %s", seed, id).isEqualTo(expected);
        }
    }

    @Test
    void proposalOnADeposedLeaderFailsAndIsNeverApplied() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode old = c.awaitLeader(Duration.ofSeconds(2));
        c.net.isolate(old.id());
        CompletableFuture<CommitResult> doomed = old.propose(Bytes.ofUtf8("lost-write"));
        List<NodeId> others = c.ids.stream().filter(id -> !id.equals(old.id())).toList();
        RaftNode replacement = c.awaitLeader(others, Duration.ofSeconds(3));
        replacement.propose(Bytes.ofUtf8("new-write"));
        c.run(Duration.ofSeconds(1));
        c.net.heal();
        c.awaitLeader(Duration.ofSeconds(3));
        c.run(Duration.ofSeconds(1));
        assertThat(doomed.isCompletedExceptionally()).as("seed %d", seed).isTrue();
        for (NodeId id : c.ids) {
            assertThat(appliedCommands(c, id)).as("seed %d", seed).isEqualTo(List.of("new-write"));
        }
    }

    /** §8: entries from an earlier term commit right after an election, without new client writes. */
    @Test
    void newLeaderCommitsEntriesFromEarlierTermsViaItsNoop() {
        assertThat(earlierTermEntryGetsApplied(Seeds.resolve(), CONFIG)).isTrue();
    }

    static boolean earlierTermEntryGetsApplied(long seed, RaftConfig config) {
        LogEntry old = new LogEntry(1, 1, EntryType.COMMAND, Bytes.ofUtf8("from-term-1"));
        Map<NodeId, List<LogEntry>> logs = Map.of(NodeId.of(1), List.of(old), NodeId.of(2), List.of(old), NodeId.of(3), List.of(old));
        SimulatedCluster c = new SimulatedCluster(seed, 3, config, logs, 1);
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(2));
        c.run(Duration.ofSeconds(2)); // no client traffic at all
        return c.runningNodes().stream().allMatch(n -> n.lastApplied() >= 1);
    }

    /**
     * §5.3 optimization: a follower with a long divergent suffix from an old term is repaired in
     * one round trip, because it reports the conflicting term and the leader skips all of it.
     * Plain one-entry-at-a-time backtracking would need ten rejections here.
     */
    @Test
    void fastBacktrackingSkipsAWholeConflictingTerm() {
        long seed = Seeds.resolve();
        List<LogEntry> leaderLog = new ArrayList<>();
        List<LogEntry> staleLog = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            LogEntry e = new LogEntry(i, 1, EntryType.COMMAND, Bytes.ofUtf8("a" + i));
            leaderLog.add(e);
            staleLog.add(e);
        }
        for (int i = 11; i <= 20; i++) {
            leaderLog.add(new LogEntry(i, 3, EntryType.COMMAND, Bytes.ofUtf8("b" + i)));
        }
        for (int i = 11; i <= 60; i++) {
            staleLog.add(new LogEntry(i, 2, EntryType.COMMAND, Bytes.ofUtf8("stale" + i)));
        }
        NodeId stale = NodeId.of(3);
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG,
                Map.of(NodeId.of(1), leaderLog, NodeId.of(2), leaderLog, stale, staleLog), 3);
        AtomicInteger rejectionsFromStale = new AtomicInteger();
        c.deliveryObserver = e -> {
            if (e.from().equals(stale) && e.message() instanceof AppendEntriesReply r && !r.success()) {
                rejectionsFromStale.incrementAndGet();
            }
        };
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(3));
        c.awaitAppliedEverywhere(leader.log().lastIndex(), Duration.ofSeconds(2));
        assertThat(leader.id()).as("seed %d: the stale node cannot win", seed).isNotEqualTo(stale);
        assertThat(c.log(stale).lastIndex()).isEqualTo(leader.log().lastIndex());
        assertThat(c.log(stale).termAt(11)).isEqualTo(3L);
        assertThat(rejectionsFromStale.get()).as("seed %d", seed).isLessThanOrEqualTo(2);
    }

    /**
     * Regression test for message amplification, found by the 20,000-seed stress run: on a
     * network that duplicates messages, a far-behind follower with tiny batches used to start
     * a new never-ending chain of AppendEntries for every duplicated reply, until the JVM ran out
     * of memory. With flow control, traffic stays proportional to the work.
     */
    @Test
    void catchUpTrafficStaysBoundedOnADuplicatingNetwork() {
        // Stop-and-wait: ~400 AppendEntries + ~400 replies + heartbeats (measured ~1,250).
        assertThat(catchUpTraffic(CONFIG.withMaxInflightAppends(1))).isLessThan(5_000L);
        // A pipeline of 8 resends more on a reordering, duplicating network (measured ~5,000-5,300
        // over 40 seeds), but it stays proportional to the work: no explosion.
        assertThat(catchUpTraffic(CONFIG)).isLessThan(12_000L);
    }

    private static long catchUpTraffic(RaftConfig config) {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, config.withMaxEntriesPerAppend(1));
        c.net.setDuplicateRate(0.2);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        NodeId down = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        c.crash(down);
        CompletableFuture<CommitResult> last = null;
        for (int i = 0; i < 400; i++) {
            last = leader.propose(Bytes.ofUtf8("w" + i));
        }
        CompletableFuture<CommitResult> lastWrite = last;
        c.sim.runUntil(lastWrite::isDone, Duration.ofSeconds(30));
        assertThat(lastWrite.isDone()).as("seed %d: writes committed", seed).isTrue();
        long sentBefore = c.net.sentCount();
        c.restart(down);                      // 400+ entries behind, one entry per message
        c.awaitAppliedEverywhere(lastWrite.join().index(), Duration.ofSeconds(60));
        return c.net.sentCount() - sentBefore;
    }

    @Test
    void singleNodeGroupCommitsAsSoonAsItsOwnLogIsSynced() {
        SimulatedCluster c = new SimulatedCluster(1, 1, CONFIG);
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(1));
        CompletableFuture<CommitResult> f = c.propose("solo");
        assertThat(f.isDone()).isFalse();          // not before its own fsync (the scheduled flush)
        c.sim.step();                              // run the flush
        assertThat(f.isDone()).isTrue();           // no network round trip needed
        assertThat(f.join().result()).isEqualTo(Bytes.ofUtf8("ok:solo"));
    }
}
