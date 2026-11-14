package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.AppendEntries;
import dev.raftkv.protocol.AppendEntriesReply;
import dev.raftkv.protocol.Message;
import dev.raftkv.protocol.RequestVote;
import dev.raftkv.protocol.RequestVoteReply;
import dev.raftkv.raft.log.InMemoryLog;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.raft.storage.InMemoryMetaStore;
import dev.raftkv.raft.storage.PersistentState;
import dev.raftkv.sim.Simulation;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;
import org.junit.jupiter.api.Test;

/**
 * Figure 2's RequestVote rules, tested on one node in isolation: we hand it messages and look
 * at exactly what it replies and what it saved.
 */
class VoteRulesTest {

    private static final NodeId ME = NodeId.of(1);
    private static final NodeId A = NodeId.of(2);
    private static final NodeId B = NodeId.of(3);
    private static final GroupId G = GroupId.of(1);

    /** What the node sent, plus a snapshot of its stored state at the moment it sent it. */
    record Sent(NodeId to, Message message, PersistentState storedAtSendTime) {}

    private final Simulation sim = new Simulation(42);
    private final InMemoryMetaStore meta;
    private final List<Sent> sent = new ArrayList<>();
    private final RaftNode node;

    /** JUnit creates a fresh instance (fresh node) for every test method. */
    VoteRulesTest() {
        meta = new InMemoryMetaStore(new PersistentState(5, null));
        Transport capture = new Transport() {
            @Override
            public NodeId self() {
                return ME;
            }

            @Override
            public void send(NodeId to, GroupId group, Message message) {
                sent.add(new Sent(to, message, meta.load()));
            }
        };
        node = RaftNode.builder().self(ME).group(G).voters(Set.of(ME, A, B)).config(RaftConfig.defaults())
                .scheduler(sim).clock(sim.clock()).random(new Random(1)).transport(capture)
                .log(new InMemoryLog(List.of(LogEntry.noop(1, 2), LogEntry.noop(2, 4)))).metaStore(meta).build();
        node.start(); // our log: [1@t2, 2@t4], currentTerm 5
    }

    private RequestVoteReply vote(NodeId candidate, long term, long lastIndex, long lastTerm) {
        sent.clear();
        node.handle(new Envelope(candidate, ME, G, new RequestVote(term, candidate, lastIndex, lastTerm, false, false)));
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).to()).isEqualTo(candidate);
        return (RequestVoteReply) sent.get(0).message();
    }

    @Test
    void rule1RejectsCandidatesFromOlderTerms() {
        RequestVoteReply r = vote(A, 4, 9, 4);
        assertThat(r.voteGranted()).isFalse();
        assertThat(r.term()).isEqualTo(5L); // tells the candidate it is behind
    }

    @Test
    void grantsAtMostOneVotePerTerm() {
        assertThat(vote(A, 6, 2, 4).voteGranted()).isTrue();
        assertThat(vote(B, 6, 2, 4).voteGranted()).isFalse();  // different candidate, same term
        assertThat(vote(A, 6, 2, 4).voteGranted()).isTrue();   // same candidate again (a retry): still yes
        assertThat(node.votedFor()).isEqualTo(Optional.of(A));
    }

    @Test
    void higherTermResetsTheVoteAndIsPersistedBeforeReplying() {
        RequestVoteReply r = vote(A, 9, 2, 4);
        assertThat(r.voteGranted()).isTrue();
        assertThat(r.term()).isEqualTo(9L);
        // At the moment the reply was sent, the store already held term 9 and our vote.
        assertThat(sent.get(0).storedAtSendTime()).isEqualTo(new PersistentState(9, A));
    }

    @Test
    void rejectsCandidateWhoseLastTermIsOlder() {
        // Our last entry is from t4. A longer log ending in t3 is still less up-to-date (§5.4.1).
        RequestVoteReply r = vote(A, 6, 100, 3);
        assertThat(r.voteGranted()).isFalse();
        assertThat(node.currentTerm()).isEqualTo(6L); // the term is still adopted
        assertThat(node.votedFor()).isEmpty();
    }

    @Test
    void rejectsCandidateWithSameLastTermButShorterLog() {
        assertThat(vote(A, 6, 1, 4).voteGranted()).isFalse();
        assertThat(vote(B, 6, 2, 4).voteGranted()).isTrue();  // same length is enough
    }

    @Test
    void grantsCandidateWithNewerLastTermEvenIfShorter() {
        assertThat(vote(A, 6, 1, 5).voteGranted()).isTrue();
    }

    @Test
    void leaderStickinessRejectsDisruptiveCandidatesWithoutAdoptingTheirTerm() {
        heartbeatFrom(A, 5);
        sim.runFor(Duration.ofMillis(100)); // still within the 150 ms minimum election timeout
        RequestVoteReply r = vote(B, 50, 2, 4);
        assertThat(r.voteGranted()).isFalse();
        assertThat(node.currentTerm()).isEqualTo(5L); // NOT bumped to 50
        assertThat(node.leaderId()).isEqualTo(Optional.of(A));
    }

    @Test
    void stickinessEndsAfterTheMinimumElectionTimeout() {
        heartbeatFrom(A, 5);
        sim.runFor(Duration.ofMillis(149));
        assertThat(vote(B, 6, 2, 4).voteGranted()).isFalse();
        sim.runFor(Duration.ofMillis(1)); // exactly 150 ms since the heartbeat
        assertThat(vote(B, 6, 2, 4).voteGranted()).isTrue();
    }

    @Test
    void leadershipTransferBypassesStickiness() {
        heartbeatFrom(A, 5);
        sent.clear();
        node.handle(new Envelope(B, ME, G, new RequestVote(6, B, 2, 4, false, true)));
        assertThat(((RequestVoteReply) sent.get(0).message()).voteGranted()).isTrue();
    }

    @Test
    void restartRemembersTheVote() {
        assertThat(vote(A, 6, 2, 4).voteGranted()).isTrue();
        node.stop();
        RaftNode restarted = RaftNode.builder().self(ME).group(G).voters(Set.of(ME, A, B)).config(RaftConfig.defaults())
                .scheduler(sim).clock(sim.clock()).random(new Random(2)).transport(captureFor())
                .log(new InMemoryLog(List.of(LogEntry.noop(1, 2), LogEntry.noop(2, 4)))).metaStore(meta).build();
        restarted.start();
        assertThat(restarted.currentTerm()).isEqualTo(6L);
        assertThat(restarted.votedFor()).isEqualTo(Optional.of(A));
        sent.clear();
        restarted.handle(new Envelope(B, ME, G, new RequestVote(6, B, 2, 4, false, false)));
        assertThat(((RequestVoteReply) sent.get(0).message()).voteGranted()).isFalse();
    }

    @Test
    void staleAppendEntriesIsRejectedWithOurTerm() {
        sent.clear();
        node.handle(new Envelope(A, ME, G, new AppendEntries(3, A, 0, 0, List.of(), 0)));
        AppendEntriesReply r = (AppendEntriesReply) sent.get(0).message();
        assertThat(r.success()).isFalse();
        assertThat(r.term()).isEqualTo(5L);
        assertThat(node.leaderId()).isEmpty();
    }

    @Test
    void heartbeatConsistencyCheckComparesPrevLogTerm() {
        sent.clear();
        node.handle(new Envelope(A, ME, G, new AppendEntries(5, A, 2, 4, List.of(), 0)));
        assertThat(((AppendEntriesReply) sent.get(0).message()).success()).isTrue();
        sent.clear();
        node.handle(new Envelope(A, ME, G, new AppendEntries(5, A, 2, 3, List.of(), 0)));
        assertThat(((AppendEntriesReply) sent.get(0).message()).success()).isFalse();
        sent.clear();
        node.handle(new Envelope(A, ME, G, new AppendEntries(5, A, 7, 4, List.of(), 0)));
        assertThat(((AppendEntriesReply) sent.get(0).message()).success()).isFalse();
    }

    @Test
    void candidateBecomesFollowerWhenALeaderOfItsTermAppears() {
        becomeCandidate();
        assertThat(node.role()).isEqualTo(Role.CANDIDATE);
        long term = node.currentTerm();
        node.handle(new Envelope(A, ME, G, new AppendEntries(term, A, 0, 0, List.of(), 0)));
        assertThat(node.role()).isEqualTo(Role.FOLLOWER);
        assertThat(node.currentTerm()).isEqualTo(term);
        assertThat(node.leaderId()).isEqualTo(Optional.of(A));
    }

    @Test
    void candidateVotesForItselfPersistsAndAsksEveryone() {
        sent.clear();
        becomeCandidate();
        assertThat(node.role()).isEqualTo(Role.CANDIDATE);
        assertThat(node.currentTerm()).isEqualTo(6L);
        assertThat(meta.load()).isEqualTo(new PersistentState(6, ME));
        List<NodeId> asked = sent.stream().filter(s -> s.message() instanceof RequestVote).map(Sent::to).distinct().sorted().toList();
        assertThat(asked).isEqualTo(List.of(A, B));
        RequestVote rv = (RequestVote) sent.get(0).message();
        assertThat(rv.lastLogIndex()).isEqualTo(2L);
        assertThat(rv.lastLogTerm()).isEqualTo(4L);
        assertThat(sent.get(0).storedAtSendTime()).isEqualTo(new PersistentState(6, ME)); // persisted first
    }

    @Test
    void candidateRetriesVoteRequestsOnlyToPeersThatHaveNotAnswered() {
        becomeCandidate();
        long term = node.currentTerm();
        node.handle(new Envelope(A, ME, G, new RequestVoteReply(term, false, false))); // A said no
        sent.clear();
        sim.runFor(Duration.ofMillis(50)); // one retry interval
        List<NodeId> retried = sent.stream().filter(s -> s.message() instanceof RequestVote).map(Sent::to).toList();
        assertThat(retried).isEqualTo(List.of(B));
    }

    @Test
    void ignoresLateRepliesFromAnOlderElection() {
        becomeCandidate();
        long term = node.currentTerm();
        node.handle(new Envelope(A, ME, G, new RequestVoteReply(term - 1, true, false)));
        node.handle(new Envelope(B, ME, G, new RequestVoteReply(term, true, true))); // a pre-vote answer
        assertThat(node.role()).isEqualTo(Role.CANDIDATE);
        node.handle(new Envelope(A, ME, G, new RequestVoteReply(term, true, false)));
        assertThat(node.role()).isEqualTo(Role.LEADER);
    }

    /**
     * Runs the simulation until the node's election timeout fires, and stops right there, so
     * the test knows exactly where it is (the next timeout is at least 150 ms away).
     */
    private void becomeCandidate() {
        assertThat(sim.runUntil(() -> node.role() == Role.CANDIDATE, Duration.ofSeconds(1))).isTrue();
    }

    private void heartbeatFrom(NodeId leader, long term) {
        node.handle(new Envelope(leader, ME, G, new AppendEntries(term, leader, 0, 0, List.of(), 0)));
    }

    private Transport captureFor() {
        return new Transport() {
            @Override
            public NodeId self() {
                return ME;
            }

            @Override
            public void send(NodeId to, GroupId group, Message message) {
                sent.add(new Sent(to, message, meta.load()));
            }
        };
    }
}
