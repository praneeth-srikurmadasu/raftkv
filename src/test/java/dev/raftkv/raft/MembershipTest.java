package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.sim.Seeds;
import dev.raftkv.sim.SimulationException;

/** Milestone 12: single-server membership changes, learners, leadership transfer (thesis ch. 3-4). */
class MembershipTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();
    private static final NodeId N1 = NodeId.of(1);
    private static final NodeId N2 = NodeId.of(2);
    private static final NodeId N3 = NodeId.of(3);
    private static final NodeId N4 = NodeId.of(4);
    private static final NodeId N5 = NodeId.of(5);

    private static SimulatedCluster threeOfFive(long seed, RaftConfig config) {
        SimulatedCluster c = new SimulatedCluster(seed, 5, config);
        c.setBootstrapVoters(List.of(N1, N2, N3));
        c.startAll();
        return c;
    }

    private static RaftNode leaderAmong(SimulatedCluster c, List<NodeId> ids) {
        return c.awaitLeader(ids, Duration.ofSeconds(3));
    }

    private static <T> T await(SimulatedCluster c, CompletableFuture<T> f) {
        return c.await(f, Duration.ofSeconds(10));
    }

    @Test
    void sparesStayLearnersAndNeverCampaign() {
        long seed = Seeds.resolve();
        SimulatedCluster c = threeOfFive(seed, CONFIG);
        leaderAmong(c, List.of(N1, N2, N3));
        c.run(Duration.ofSeconds(3));
        for (NodeId spare : List.of(N4, N5)) {
            assertThat(c.node(spare).role()).as("seed %d", seed).isEqualTo(Role.LEARNER);
            assertThat(c.node(spare).electionsStarted() + c.node(spare).preVotesStarted()).isEqualTo(0L);
        }
    }

    @Test
    void addServerJoinsAsALearnerThenBecomesAVoter() {
        long seed = Seeds.resolve();
        SimulatedCluster c = threeOfFive(seed, CONFIG);
        RaftNode leader = leaderAmong(c, List.of(N1, N2, N3));
        for (int i = 0; i < 50; i++) {
            leader.propose(Bytes.ofUtf8("before-" + i));
        }
        await(c, leader.addServer(N4));
        assertThat(leader.voters()).isEqualTo(Set.of(N1, N2, N3, N4));
        long target = leader.commitIndex();
        c.sim.runUntil(() -> c.node(N4).lastApplied() >= target, Duration.ofSeconds(5));
        assertThat(c.node(N4).role()).as("seed %d", seed).isEqualTo(Role.FOLLOWER);
        assertThat(c.node(N4).voters()).isEqualTo(Set.of(N1, N2, N3, N4));
        assertThat(c.node(N5).role()).isEqualTo(Role.LEARNER); // never added
        // Adding again is a no-op.
        await(c, leader.addServer(N4));
        assertThat(leader.voters()).isEqualTo(Set.of(N1, N2, N3, N4));
    }

    @Test
    void threeToFiveToThreeWhileWritesKeepCommitting() {
        long seed = Seeds.resolve();
        SimulatedCluster c = threeOfFive(seed, CONFIG.withSnapshots(20, 512));
        int[] written = {0};
        Runnable[] writer = new Runnable[1];
        writer[0] = () -> {
            c.runningNodes().stream().filter(n -> n.role() == Role.LEADER).findFirst()
                    .ifPresent(l -> l.propose(Bytes.ofUtf8("w" + written[0]++)));
            c.sim.schedule(Duration.ofMillis(5), writer[0]);
        };
        c.sim.execute(writer[0]);
        for (NodeId add : List.of(N4, N5)) {
            RaftNode leader = c.awaitLeader(List.of(N1, N2, N3), Duration.ofSeconds(3));
            await(c, leader.addServer(add));
        }
        assertThat(c.awaitLeader(List.of(N1, N2, N3), Duration.ofSeconds(3)).voters()).isEqualTo(Set.of(N1, N2, N3, N4, N5));
        java.util.TreeSet<NodeId> voters = new java.util.TreeSet<>(List.of(N1, N2, N3, N4, N5));
        for (NodeId remove : List.of(N1, N2)) {
            RaftNode leader = c.awaitLeader(List.copyOf(voters), Duration.ofSeconds(3));
            await(c, leader.removeServer(remove));
            voters.remove(remove);
        }
        RaftNode leader = c.awaitLeader(List.of(N3, N4, N5), Duration.ofSeconds(3));
        assertThat(leader.voters()).as("seed %d", seed).isEqualTo(Set.of(N3, N4, N5));
        long before = leader.commitIndex();
        c.run(Duration.ofSeconds(1));
        assertThat(leader.commitIndex()).as("writes still commit with the new majority").isGreaterThan(before + 20);
        assertThat(written[0]).isGreaterThan(200);
    }

    @Test
    void aRemovedLeaderStepsDownOnceTheChangeCommits() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        await(c, leader.removeServer(leader.id()));
        assertThat(leader.role()).as("seed %d", seed).isEqualTo(Role.LEARNER);
        List<NodeId> rest = c.ids.stream().filter(id -> !id.equals(leader.id())).toList();
        RaftNode next = c.awaitLeader(rest, Duration.ofSeconds(3));
        assertThat(next.voters()).isEqualTo(Set.copyOf(rest));
        await(c, next.propose(Bytes.ofUtf8("after")));
    }

    @Test
    void oneChangeAtATimeAndOnlyOneServerAtATime() {
        long seed = Seeds.resolve();
        SimulatedCluster c = threeOfFive(seed, CONFIG);
        RaftNode leader = leaderAmong(c, List.of(N1, N2, N3));
        c.sim.runUntil(() -> leader.commitIndex() >= 1, Duration.ofSeconds(2));
        CompletableFuture<CommitResult> first = leader.changeMembership(leader.membership().withLearner(N4));
        // The first change is in effect as soon as it is appended, so this one builds on it; but it
        // must wait until the first has committed before it is appended itself.
        CompletableFuture<CommitResult> second = leader.changeMembership(leader.membership().withLearner(N5));
        assertThat(second.isDone()).as("waits while the first change is uncommitted").isFalse();
        long firstIndex = await(c, first).index();
        long secondIndex = await(c, second).index();
        assertThat(secondIndex).as("seed %d", seed).isGreaterThan(firstIndex);
        assertThat(leader.membership().learners()).isEqualTo(Set.of(N4, N5));
        Membership twoAtOnce = leader.membership().without(N5).promoted(N4);
        assertThatThrownBy(() -> leader.changeMembership(twoAtOnce).join()).hasMessageContaining("single-server");
    }

    @Test
    void leadershipTransferHandsOverWithinAnElectionTimeout() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        for (int i = 0; i < 30; i++) {
            leader.propose(Bytes.ofUtf8("x" + i));
        }
        NodeId target = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        long start = c.sim.now();
        CompletableFuture<Void> transfer = leader.transferLeadership(target);
        assertThat(leader.propose(Bytes.ofUtf8("during")).isCompletedExceptionally()).as("no new work while transferring").isTrue();
        await(c, transfer);
        c.sim.runUntil(() -> c.node(target).role() == Role.LEADER, Duration.ofMillis(100));
        assertThat(c.node(target).role()).as("seed %d", seed).isEqualTo(Role.LEADER);
        assertThat(c.sim.now() - start).isLessThan(Duration.ofMillis(150).toNanos());
    }

    @Test
    void aTransferToAnUnreachableServerTimesOutAndTheLeaderCarriesOn() {
        long seed = Seeds.resolve();
        SimulatedCluster c = new SimulatedCluster(seed, 3, CONFIG);
        c.startAll();
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(2));
        NodeId target = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        c.net.isolate(target);
        CompletableFuture<Void> transfer = leader.transferLeadership(target);
        c.run(Duration.ofMillis(400));
        assertThat(transfer.isCompletedExceptionally()).as("seed %d", seed).isTrue();
        assertThat(leader.role()).isEqualTo(Role.LEADER);
        await(c, leader.propose(Bytes.ofUtf8("still working")));
    }

    @Test
    void membershipSurvivesSnapshotsAndRestarts() {
        long seed = Seeds.resolve();
        SimulatedCluster c = threeOfFive(seed, CONFIG.withSnapshots(10, 256));
        RaftNode leader = leaderAmong(c, List.of(N1, N2, N3));
        await(c, leader.addServer(N4));
        for (int i = 0; i < 60; i++) {
            leader.propose(Bytes.ofUtf8("fill" + i)); // several snapshots: the config entry is compacted away
        }
        long target = leader.commitIndex();
        c.sim.runUntil(() -> List.of(N1, N2, N3, N4).stream().allMatch(id -> c.node(id).lastApplied() >= target),
                Duration.ofSeconds(5));
        for (NodeId id : List.of(N1, N2, N3, N4)) {
            assertThat(c.log(id).snapshotIndex()).as("compacted on %s", id).isGreaterThan(leader.log().snapshotIndex() - 40);
            c.crash(id);
            c.restart(id);
            assertThat(c.node(id).voters()).as("seed %d %s", seed, id).isEqualTo(Set.of(N1, N2, N3, N4));
        }
        RaftNode after = c.awaitLeader(List.of(N1, N2, N3, N4), Duration.ofSeconds(3));
        await(c, after.propose(Bytes.ofUtf8("after restarts")));
    }

    /**
     * The single-server membership bug Diego Ongaro reported on raft-dev in 2015, reproduced step by
     * step. Without the rule "commit an entry of your own term before changing the membership",
     * two leaders with different configurations commit conflicting entries, and a committed entry
     * is overwritten. The invariant checker must stop the run; with the rule, the change is refused.
     */
    @Test
    void theMembershipChangeSafetyRuleIsEnforcedAndItsBugIsCaught() {
        for (long seed = 1; seed <= 30; seed++) {
            String outcome = ongaroScenario(seed, CONFIG.withBugs(Bug.CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT));
            if (outcome.equals("violation")) {
                System.out.println("[bug-detection] CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT: caught at seed " + seed);
                for (long s = 1; s <= 30; s++) {
                    assertThat(ongaroScenario(s, CONFIG)).as("correct code, seed %d", s).isNotEqualTo("violation");
                }
                return;
            }
        }
        throw new AssertionError("CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT not caught in 30 seeds");
    }

    /**
     * Voters {1,2,3,4}; node 5 a learner. (1) Leader A appends "add 5 as voter", which reaches only 5.
     * (2) Cut off from A, the others elect B; B removes one of them, R, before its own no-op has
     * committed, and the removal commits on B and the remaining M (a majority of {1,2,3,4} minus R).
     * (3) A, R and 5 meet: A wins with them (a majority of {1,..,5}), never having seen B's entries,
     * and overwrites them. Returns "violation", "refused" (the rule held), or "skipped".
     */
    private static String ongaroScenario(long seed, RaftConfig config) {
        SimulatedCluster c = new SimulatedCluster(seed, 5, config.withMaxInflightAppends(1));
        c.setBootstrapVoters(List.of(N1, N2, N3, N4));
        c.startAll();
        try {
            RaftNode a = c.awaitLeader(List.of(N1, N2, N3, N4), Duration.ofSeconds(3));
            await(c, a.changeMembership(a.membership().withLearner(N5)));
            long added = a.commitIndex();
            c.sim.runUntil(() -> c.ids.stream().allMatch(id -> c.node(id).lastApplied() >= added), Duration.ofSeconds(3));
            List<NodeId> others = c.ids.stream().filter(id -> !id.equals(a.id()) && !id.equals(N5)).toList();
            c.net.partition(List.of(a.id(), N5), others);
            a.changeMembership(a.membership().promoted(N5)); // reaches only 5: never commits
            c.run(Duration.ofMillis(50));

            long termBefore = a.currentTerm();
            boolean elected = c.sim.runUntil(() -> others.stream()
                    .anyMatch(id -> c.node(id).role() == Role.LEADER && c.node(id).currentTerm() > termBefore), Duration.ofSeconds(3));
            if (!elected) {
                return "skipped";
            }
            RaftNode b = others.stream().map(c::node).filter(n -> n.role() == Role.LEADER).findFirst().orElseThrow();
            NodeId r = others.stream().filter(id -> !id.equals(b.id())).findFirst().orElseThrow();
            NodeId m = others.stream().filter(id -> !id.equals(b.id()) && !id.equals(r)).findFirst().orElseThrow();
            // R must not get B's entries: cut it off at once (B's first messages are still in flight).
            c.net.partition(List.of(a.id(), N5), List.of(r), List.of(b.id(), m));
            CompletableFuture<CommitResult> removal = b.removeServer(r);
            if (removal.isCompletedExceptionally()) {
                return "refused";
            }
            boolean committed = c.sim.runUntil(removal::isDone, Duration.ofSeconds(2));
            if (!committed || removal.isCompletedExceptionally()) {
                return "skipped";
            }
            // A, R and 5 meet; B and M are cut off.
            c.net.partition(List.of(a.id(), r, N5), List.of(b.id(), m));
            boolean oldSideWins = c.sim.runUntil(() -> List.of(a.id(), r, N5).stream()
                    .anyMatch(id -> c.node(id).role() == Role.LEADER), Duration.ofSeconds(5));
            if (!oldSideWins) {
                return "skipped";
            }
            c.net.heal();
            c.run(Duration.ofSeconds(3)); // the old side's leader overwrites B's committed entries
            return "survived";
        } catch (SimulationException e) {
            return "violation";
        }
    }
}
