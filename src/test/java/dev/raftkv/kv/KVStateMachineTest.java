package dev.raftkv.kv;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import dev.raftkv.common.Bytes;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.raft.Bug;
import org.junit.jupiter.api.Test;

/** The KV store and its exactly-once session rules, applied entry by entry. */
class KVStateMachineTest {

    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);

    private final KVStateMachine sm = new KVStateMachine(Duration.ofSeconds(10), Set.of());
    private long index;

    private static Bytes b(String s) {
        return Bytes.ofUtf8(s);
    }

    /** Applies one client request as the next log entry; returns the decoded outcome. */
    private KVCodec.Outcome apply(KVStateMachine target, UUID client, long seq, long ackedBelow, long timeMillis, KVCommand cmd) {
        Bytes payload = KVCodec.encode(new SessionCommand(client, seq, ackedBelow, timeMillis, KVCodec.encode(cmd)));
        return KVCodec.decodeOutcome(target.apply(++index, payload));
    }

    private KVCodec.Outcome apply(UUID client, long seq, long ackedBelow, KVCommand cmd) {
        return apply(sm, client, seq, ackedBelow, 1000, cmd);
    }

    private static KVResult result(KVCodec.Outcome o) {
        assertThat(o.status()).isEqualTo(ClientStatus.OK);
        return KVCodec.decodeResult(o.result());
    }

    @Test
    void putGetDeleteAndCasBehaveLikeAMap() {
        assertThat(result(apply(A, 1, 1, new KVCommand.Put(b("k"), b("v1"))))).isEqualTo(new KVResult.PutResult(Optional.empty()));
        assertThat(result(apply(A, 2, 2, new KVCommand.Put(b("k"), b("v2"))))).isEqualTo(new KVResult.PutResult(Optional.of(b("v1"))));
        assertThat(result(apply(A, 3, 3, new KVCommand.Get(b("k"))))).isEqualTo(new KVResult.GetResult(Optional.of(b("v2"))));
        assertThat(result(apply(A, 4, 4, new KVCommand.Cas(b("k"), Optional.of(b("nope")), b("v3")))))
                .isEqualTo(new KVResult.CasResult(false, Optional.of(b("v2"))));
        assertThat(result(apply(A, 5, 5, new KVCommand.Cas(b("k"), Optional.of(b("v2")), b("v3")))))
                .isEqualTo(new KVResult.CasResult(true, Optional.of(b("v3"))));
        assertThat(result(apply(A, 6, 6, new KVCommand.Cas(b("new"), Optional.empty(), b("x")))))
                .isEqualTo(new KVResult.CasResult(true, Optional.of(b("x"))));      // "only if absent"
        assertThat(result(apply(A, 7, 7, new KVCommand.Delete(b("k"))))).isEqualTo(new KVResult.DeleteResult(true));
        assertThat(result(apply(A, 8, 8, new KVCommand.Delete(b("k"))))).isEqualTo(new KVResult.DeleteResult(false));
        assertThat(sm.size()).isEqualTo(1);
    }

    /** The heart of exactly-once: a retry gets the ORIGINAL answer and changes nothing. */
    @Test
    void aRetryReturnsTheCachedResultWithoutExecutingAgain() {
        KVCodec.Outcome first = apply(A, 1, 1, new KVCommand.Put(b("k"), b("A")));
        apply(B, 1, 1, new KVCommand.Put(b("k"), b("B")));          // someone else writes in between
        KVCodec.Outcome retry = apply(A, 1, 1, new KVCommand.Put(b("k"), b("A")));
        assertThat(retry).isEqualTo(first);                           // same answer as the first time
        assertThat(sm.peek(b("k"))).isEqualTo(Optional.of(b("B")));   // and B's write was not overwritten
        assertThat(sm.executedCommands()).isEqualTo(2L);
    }

    @Test
    void outOfOrderRequestsFromOneClientEachExecuteOnce() {
        apply(A, 2, 1, new KVCommand.Put(b("x"), b("2")));  // seq 2 arrives first (pipelined client)
        apply(A, 1, 1, new KVCommand.Put(b("y"), b("1")));
        apply(A, 2, 1, new KVCommand.Put(b("x"), b("2")));  // retry of 2
        assertThat(sm.executedCommands()).isEqualTo(2L);
    }

    @Test
    void theWatermarkForgetsAcknowledgedResults() {
        apply(A, 1, 1, new KVCommand.Put(b("k"), b("1")));
        apply(A, 2, 2, new KVCommand.Put(b("k"), b("2")));   // ackedBelow=2: the client has reply 1
        KVCodec.Outcome stale = apply(A, 1, 1, new KVCommand.Put(b("k"), b("1"))); // a very late duplicate of 1
        assertThat(stale.status()).isEqualTo(ClientStatus.SESSION_EXPIRED);       // refused, not executed
        assertThat(sm.peek(b("k"))).isEqualTo(Optional.of(b("2")));
        assertThat(sm.executedCommands()).isEqualTo(2L);
    }

    @Test
    void anUnknownClientThatHasSeenRepliesHasAnExpiredSession() {
        KVCodec.Outcome o = apply(A, 5, 3, new KVCommand.Put(b("k"), b("v")));
        assertThat(o.status()).isEqualTo(ClientStatus.SESSION_EXPIRED);
        assertThat(sm.executedCommands()).isEqualTo(0L);
        assertThat(sm.sessionCount()).isEqualTo(0);
    }

    /** Expiry uses timestamps from the log, so every replica forgets the same session at the same entry. */
    @Test
    void sessionsExpireByLogTimeAndTimeNeverRunsBackwards() {
        apply(sm, A, 1, 1, 1_000, new KVCommand.Put(b("a"), b("1")));
        apply(sm, B, 1, 1, 5_000, new KVCommand.Put(b("b"), b("1")));
        // A new leader with a slower clock stamps an older time: logical time must not go back.
        apply(sm, B, 2, 2, 2_000, new KVCommand.Put(b("b"), b("2")));
        apply(sm, B, 3, 3, 12_500, new KVCommand.Put(b("b"), b("3"))); // 11.5 s after A's last activity
        assertThat(sm.sessionCount()).isEqualTo(1);                      // A expired, B alive
        KVCodec.Outcome late = apply(sm, A, 2, 2, 12_600, new KVCommand.Put(b("a"), b("2")));
        assertThat(late.status()).isEqualTo(ClientStatus.SESSION_EXPIRED);
        assertThat(sm.peek(b("a"))).isEqualTo(Optional.of(b("1")));
    }

    @Test
    void garbageNeverThrowsItBecomesInvalidRequest() {
        Bytes garbage = Bytes.copyOf(new byte[] {7, 7, 7});
        assertThat(KVCodec.decodeOutcome(sm.apply(1, garbage)).status()).isEqualTo(ClientStatus.INVALID_REQUEST);
        Bytes badCommand = KVCodec.encode(new SessionCommand(A, 1, 1, 0, Bytes.copyOf(new byte[] {42})));
        assertThat(KVCodec.decodeOutcome(sm.apply(2, badCommand)).status()).isEqualTo(ClientStatus.INVALID_REQUEST);
    }

    /** Determinism: two replicas fed the same entries end in byte-identical states. */
    @Test
    void replicasThatApplyTheSameEntriesAreIdentical() {
        for (long seed = 0; seed < 50; seed++) {
            KVStateMachine r1 = new KVStateMachine(Duration.ofSeconds(5), Set.of());
            KVStateMachine r2 = new KVStateMachine(Duration.ofSeconds(5), Set.of());
            Random r = new Random(seed);
            List<UUID> clients = List.of(A, B, new UUID(0, 3));
            long[] seq = new long[3];
            long time = 0;
            index = 0;
            List<Bytes> entries = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                int c = r.nextInt(3);
                boolean retry = seq[c] > 0 && r.nextInt(4) == 0;
                long s = retry ? seq[c] : ++seq[c];
                time += r.nextInt(500) - 100; // clocks jitter, sometimes backwards
                KVCommand cmd = KVCodecTest.randomCommand(r);
                entries.add(KVCodec.encode(new SessionCommand(clients.get(c), s, Math.max(1, s - r.nextInt(3)), time,
                        KVCodec.encode(cmd))));
            }
            for (int i = 0; i < entries.size(); i++) {
                r1.apply(i + 1, entries.get(i));
                r2.apply(i + 1, entries.get(i));
            }
            assertThat(r1.digest()).as("seed %d", seed).isEqualTo(r2.digest());
        }
        KVStateMachine other = new KVStateMachine(Duration.ofSeconds(5), Set.of());
        apply(other, A, 1, 1, 0, new KVCommand.Put(b("k"), b("different")));
        assertThat(other.digest()).isNotEqualTo(new KVStateMachine(Duration.ofSeconds(5), Set.of()).digest());
    }

    @Test
    void withDedupDisabledARetryExecutesTwice() {
        KVStateMachine buggy = new KVStateMachine(Duration.ofSeconds(10), Set.of(Bug.SESSION_DEDUP_DISABLED));
        apply(buggy, A, 1, 1, 0, new KVCommand.Put(b("k"), b("v")));
        KVCodec.Outcome retry = apply(buggy, A, 1, 1, 0, new KVCommand.Put(b("k"), b("v")));
        assertThat(buggy.executedCommands()).isEqualTo(2L);
        // The visible symptom: the retry's "previous value" is the write itself.
        assertThat(KVCodec.decodeResult(retry.result())).isEqualTo(new KVResult.PutResult(Optional.of(b("v"))));
    }

    @Test
    void aRestoredSnapshotBehavesExactlyLikeTheOriginal() {
        apply(A, 1, 1, new KVCommand.Put(b("x"), b("1")));
        apply(A, 2, 1, new KVCommand.Put(b("y"), b("2")));
        apply(B, 1, 1, new KVCommand.Cas(b("x"), java.util.Optional.of(b("1")), b("3")));
        KVStateMachine copy = new KVStateMachine(Duration.ofSeconds(10), Set.of());
        copy.apply(1, KVCodec.encode(new SessionCommand(B, 9, 1, 1, KVCodec.encode(new KVCommand.Put(b("junk"), b("!"))))));
        copy.restore(sm.snapshot()); // replaces everything, including the junk
        assertThat(copy.digest()).isEqualTo(sm.digest());
        assertThat(copy.size()).isEqualTo(2);
        assertThat(copy.executedCommands()).isEqualTo(sm.executedCommands());
        assertThat(copy.peek(b("junk"))).isEqualTo(java.util.Optional.empty());

        // A retry after the restore is still answered from the session cache, not executed again.
        long executedBefore = copy.executedCommands();
        KVCodec.Outcome retried = apply(copy, A, 2, 1, 0, new KVCommand.Put(b("y"), b("2")));
        assertThat(result(retried)).isEqualTo(new KVResult.PutResult(java.util.Optional.empty()));
        assertThat(copy.executedCommands()).isEqualTo(executedBefore);

        // And both keep evolving identically.
        apply(sm, A, 3, 3, 0, new KVCommand.Delete(b("y")));
        apply(copy, A, 3, 3, 0, new KVCommand.Delete(b("y")));
        assertThat(copy.digest()).isEqualTo(sm.digest());
        assertThat(copy.snapshot()).isEqualTo(sm.snapshot());
    }

    @Test
    void aCorruptSnapshotIsRejectedWithoutChangingState() {
        apply(A, 1, 1, new KVCommand.Put(b("x"), b("1")));
        long digest = sm.digest();
        Bytes snap = sm.snapshot();
        Bytes truncated = snap.slice(0, snap.length() - 3);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sm.restore(truncated))
                .isInstanceOf(dev.raftkv.codec.CodecException.class);
        assertThat(sm.digest()).isEqualTo(digest);
    }
}
