package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.AppendEntries;
import dev.raftkv.protocol.AppendEntriesReply;
import dev.raftkv.protocol.Message;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.InMemoryLog;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.raft.storage.InMemoryMetaStore;
import dev.raftkv.raft.storage.PersistentState;
import dev.raftkv.sim.Simulation;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;
import org.junit.jupiter.api.Test;

/** Figure 2's AppendEntries receiver rules 2-5 on one follower, in isolation. */
class FollowerAppendRulesTest {

    private static final NodeId ME = NodeId.of(1);
    private static final NodeId LEADER = NodeId.of(2);
    private static final GroupId G = GroupId.of(1);

    private final Simulation sim = new Simulation(7);
    private final List<Message> sent = new ArrayList<>();
    private final List<String> applied = new ArrayList<>();
    private InMemoryLog log;
    private RaftNode node;

    private void startWith(long term, LogEntry... entries) {
        startWith(RaftConfig.defaults(), term, entries);
    }

    void startWith(RaftConfig config, long term, LogEntry... entries) {
        log = new InMemoryLog(List.of(entries));
        Transport capture = new Transport() {
            @Override
            public NodeId self() {
                return ME;
            }

            @Override
            public void send(NodeId to, GroupId group, Message message) {
                sent.add(message);
            }
        };
        node = RaftNode.builder().self(ME).group(G).voters(Set.of(ME, LEADER, NodeId.of(3))).config(config)
                .scheduler(sim).clock(sim.clock()).random(new Random(1)).transport(capture)
                .log(log).metaStore(new InMemoryMetaStore(new PersistentState(term, null)))
                .stateMachine((index, cmd) -> {
                    applied.add(index + ":" + cmd.toUtf8());
                    return Bytes.EMPTY;
                })
                .build();
        node.start();
    }

    static LogEntry e(long index, long term) {
        return new LogEntry(index, term, EntryType.COMMAND, Bytes.ofUtf8("c" + index + "t" + term));
    }

    AppendEntriesReply append(long term, long prevIndex, long prevTerm, long leaderCommit, LogEntry... entries) {
        sent.clear();
        node.handle(new Envelope(LEADER, ME, G, new AppendEntries(term, LEADER, prevIndex, prevTerm, List.of(entries), leaderCommit)));
        assertThat(sent).hasSize(1);
        return (AppendEntriesReply) sent.get(0);
    }

    private List<Long> terms() {
        List<Long> t = new ArrayList<>();
        for (long i = 1; i <= log.lastIndex(); i++) {
            t.add(log.termAt(i));
        }
        return t;
    }

    @Test
    void appendsNewEntriesAndReportsTheMatchIndex() {
        startWith(1);
        AppendEntriesReply r = append(1, 0, 0, 0, e(1, 1), e(2, 1));
        assertThat(r.success()).isTrue();
        assertThat(r.matchIndex()).isEqualTo(2L);
        assertThat(terms()).isEqualTo(List.of(1L, 1L));
    }

    @Test
    void duplicateMessageChangesNothing() {
        startWith(1);
        append(1, 0, 0, 0, e(1, 1), e(2, 1));
        AppendEntriesReply r = append(1, 0, 0, 0, e(1, 1), e(2, 1));
        assertThat(r.success()).isTrue();
        assertThat(log.lastIndex()).isEqualTo(2L);
    }

    /** The classic bug: a delayed, shorter AppendEntries must not delete newer entries. */
    @Test
    void staleShorterMessageDoesNotTruncate() {
        startWith(1);
        append(1, 0, 0, 0, e(1, 1), e(2, 1), e(3, 1), e(4, 1), e(5, 1));
        AppendEntriesReply r = append(1, 1, 1, 0, e(2, 1));   // an older message arriving late
        assertThat(r.success()).isTrue();
        assertThat(r.matchIndex()).isEqualTo(2L);              // only what this message proves
        assertThat(log.lastIndex()).isEqualTo(5L);              // nothing deleted
    }

    @Test
    void conflictingEntriesAreReplacedFromTheFirstConflict() {
        startWith(2, e(1, 1), e(2, 1), e(3, 2), e(4, 2));
        AppendEntriesReply r = append(3, 1, 1, 0, e(2, 1), e(3, 3));
        assertThat(r.success()).isTrue();
        assertThat(terms()).isEqualTo(List.of(1L, 1L, 3L));    // 3@t2 and 4@t2 gone, 3@t3 in
    }

    @Test
    void tooShortLogAsksToRetryFromItsEnd() {
        startWith(1, e(1, 1), e(2, 1));
        AppendEntriesReply r = append(1, 6, 1, 0);
        assertThat(r.success()).isFalse();
        assertThat(r.conflictIndex()).isEqualTo(3L);
        assertThat(r.conflictTerm()).isEqualTo(0L);
    }

    @Test
    void termMismatchReportsTheConflictingTermAndWhereItStarts() {
        startWith(3, e(1, 1), e(2, 2), e(3, 2), e(4, 2), e(5, 2));
        AppendEntriesReply r = append(3, 4, 3, 0);
        assertThat(r.success()).isFalse();
        assertThat(r.conflictTerm()).isEqualTo(2L);
        assertThat(r.conflictIndex()).isEqualTo(2L);            // first index holding t2
    }

    long commitIndex() {
        return node.commitIndex();
    }

    /** Rule 5 uses the index of the last NEW entry, not our last index: the rest is unverified. */
    @Test
    void commitIndexIsCappedByTheLastVerifiedEntry() {
        startWith(1, e(1, 1), e(2, 1), e(3, 1));
        append(1, 1, 1, 3);                                     // heartbeat proving only index 1
        assertThat(node.commitIndex()).isEqualTo(1L);
        assertThat(applied).isEqualTo(List.of("1:c1t1"));
        append(1, 3, 1, 3);                                     // now index 3 is proven
        assertThat(node.commitIndex()).isEqualTo(3L);
        assertThat(applied).isEqualTo(List.of("1:c1t1", "2:c2t1", "3:c3t1"));
    }

    @Test
    void commitIndexNeverGoesBackwards() {
        startWith(1);
        append(1, 0, 0, 2, e(1, 1), e(2, 1));
        append(1, 0, 0, 1);                                     // an old heartbeat with a smaller leaderCommit
        assertThat(node.commitIndex()).isEqualTo(2L);
    }

    @Test
    void noopEntriesAreNotPassedToTheStateMachine() {
        startWith(1);
        append(1, 0, 0, 2, LogEntry.noop(1, 1), e(2, 1));
        assertThat(applied).isEqualTo(List.of("2:c2t1"));
        assertThat(node.lastApplied()).isEqualTo(2L);
    }
}
