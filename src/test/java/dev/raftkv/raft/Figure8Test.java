package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.sim.SimulationException;
import org.junit.jupiter.api.Test;

/**
 * The Figure 8 scenario of the Raft paper (§5.4.2), reproduced step by step.
 *
 * <pre>
 *   index:        1      2
 *   S1, S2:      x@1   y2@2      (term-2 leader S1 replicated y2 to S2 only, then crashed)
 *   S3, S4:      x@1
 *   S5:          x@1   y3@3      (term-3 leader S5 wrote y3 locally, then crashed)
 * </pre>
 *
 * Now S1 or S2 becomes leader of term 4 and copies y2 to S3, so y2 is stored on a MAJORITY
 * (S1, S2, S3). Is y2 committed? <b>No</b>: if the term-4 leader crashes before an entry of
 * term 4 reaches a majority, S5 can still win (its last term, 3, beats S3's and S4's) and
 * overwrite index 2 with y3. So a leader may only commit by counting replicas for entries of
 * its <em>own</em> term.
 */
class Figure8Test {

    /**
     * The scripted Figure 8 history forces elections at exact moments: classic Raft elections
     * (no pre-vote, which would ask permission first; no check-quorum, which would depose the
     * scripted leaders early), and stop-and-wait replication, so each scripted step replicates
     * exactly what the script expects. The safety rule under test does not depend on any of these.
     */
    static final RaftConfig CLASSIC = RaftConfig.defaults().withElectionExtensions(false, false).withMaxInflightAppends(1);

    private static final NodeId S1 = NodeId.of(1);
    private static final NodeId S2 = NodeId.of(2);
    private static final NodeId S3 = NodeId.of(3);
    private static final NodeId S4 = NodeId.of(4);
    private static final NodeId S5 = NodeId.of(5);

    private static LogEntry cmd(long index, long term, String text) {
        return new LogEntry(index, term, EntryType.COMMAND, Bytes.ofUtf8(text));
    }

    /** Plays the scenario; returns the cluster at the end. Throws if a safety check fails. */
    static SimulatedCluster play(long seed, RaftConfig config) {
        LogEntry x = cmd(1, 1, "x");
        Map<NodeId, List<LogEntry>> logs = Map.of(
                S1, List.of(x, cmd(2, 2, "y2")),
                S2, List.of(x, cmd(2, 2, "y2")),
                S3, List.of(x),
                S4, List.of(x),
                S5, List.of(x, cmd(2, 3, "y3")));
        // One entry per message, so we can stop between "y2 reached S3" and "the term-4 no-op did".
        SimulatedCluster c = new SimulatedCluster(seed, 5, config.withMaxEntriesPerAppend(1), logs, 3);
        c.net.setLatency(Duration.ofMillis(2), Duration.ofMillis(8));
        c.startAllExcept(List.of(S5));
        c.net.partition(List.of(S1, S2, S3), List.of(S4, S5)); // S4 sits this phase out

        // (c) S1 or S2 wins term 4 (S3 can't: its log is behind theirs).
        RaftNode leader = c.awaitLeader(List.of(S1, S2, S3), Duration.ofSeconds(3));
        assertThat(leader.id()).isIn(S1, S2);
        // Run until the leader learns S3 stores index 2: y2 is now on a majority.
        assertThat(c.sim.runUntil(() -> leader.matchIndexOf(S3).orElse(0L) >= 2, Duration.ofSeconds(2))).isTrue();
        long committedAtThatMoment = leader.commitIndex();

        // (d) The term-4 leader and its twin crash before the no-op reaches a majority.
        c.crash(S1);
        c.crash(S2);
        c.net.heal();
        c.restart(S5);
        RaftNode s5 = c.awaitLeader(List.of(S3, S4, S5), Duration.ofSeconds(3));
        assertThat(s5.id()).isEqualTo(S5); // legitimate: index 2 was never committed
        c.awaitAppliedEverywhere(s5.log().lastIndex(), Duration.ofSeconds(3));

        c.restart(S1);
        c.restart(S2);
        RaftNode finalLeader = c.awaitLeader(Duration.ofSeconds(3));
        c.awaitAppliedEverywhere(finalLeader.log().lastIndex(), Duration.ofSeconds(3));
        assertThat(committedAtThatMoment).as("y2 was on a majority but must not count as committed").isEqualTo(0L);
        return c;
    }

    @Test
    void correctLeaderDoesNotCommitAnEarlierTermEntryByCounting() {
        for (long seed = 1; seed <= 20; seed++) {
            SimulatedCluster c = play(seed, CLASSIC);
            // Everyone ends up with y3 at index 2 (S5's entry won), and applied it.
            for (NodeId id : c.ids) {
                assertThat(c.log(id).entryAt(2).data()).as("seed %d %s", seed, id).isEqualTo(Bytes.ofUtf8("y3"));
                assertThat(c.stateMachine(id).applied().get(1).command()).isEqualTo(Bytes.ofUtf8("y3"));
            }
        }
    }

    @Test
    void theBugIsCaughtWhenCountingReplicasOfOldEntries() {
        // With the bug, the term-4 leader commits and APPLIES y2 at index 2; later S5's group
        // applies y3 at index 2. State Machine Safety (or "committed entry changed") fires.
        assertThatThrownBy(() -> play(1, CLASSIC.withBugs(Bug.COMMIT_PREVIOUS_TERM_BY_COUNT)))
                .isInstanceOf(SimulationException.class)
                .hasMessageContaining("seed=1");
    }
}
