package dev.raftkv.kv;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Hashing;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.raft.Bug;
import dev.raftkv.raft.StateMachine;

/**
 * The replicated key-value store (SPEC §4.3): a sorted map of byte keys to byte values, plus the
 * {@link SessionTable} that makes client retries exactly-once.
 *
 * <p>For every committed entry Raft calls {@link #apply}, which:
 * <ol>
 *   <li>decodes the {@link SessionCommand} (garbage becomes INVALID_REQUEST, never an exception:
 *       an exception would crash every replica at the same entry);</li>
 *   <li>advances logical time to {@code max(lastTime, entry timestamp)} and expires idle
 *       sessions using <em>that</em> time, so every replica expires the same sessions at the same
 *       log index (local clocks would make replicas diverge);</li>
 *   <li>applies the session rules (see {@link #applySession});</li>
 *   <li>executes the KV operation and caches its result in the session.</li>
 * </ol>
 * Everything here must be deterministic: same entries in the same order give byte-for-byte the
 * same state on every replica, which {@link #digest()} lets tests verify.
 */
public final class KVStateMachine implements StateMachine {

    /** How often (in log time) idle sessions are swept. */
    private static final long EXPIRY_SWEEP_MILLIS = 1_000;

    private final TreeMap<Bytes, Bytes> data = new TreeMap<>();
    private final SessionTable sessions = new SessionTable();
    private final long sessionTimeoutMillis;
    private final Set<Bug> bugs;
    private long logTimeMillis;
    private long nextSweepMillis;
    private long executed;
    private BiConsumer<UUID, Long> executionObserver = (client, seq) -> { };

    /** Default session timeout: one hour of log time. */
    public KVStateMachine() {
        this(Duration.ofHours(1), Set.of());
    }

    public KVStateMachine(Duration sessionTimeout, Set<Bug> bugs) {
        this.sessionTimeoutMillis = sessionTimeout.toMillis();
        this.bugs = Set.copyOf(bugs);
    }

    @Override
    public Bytes apply(long index, Bytes payload) {
        SessionCommand sc;
        try {
            sc = KVCodec.decodeSessionCommand(payload);
        } catch (CodecException e) {
            return outcome(ClientStatus.INVALID_REQUEST, Bytes.EMPTY);
        }
        // Logical time only moves forward, even if a new leader's clock is behind the old one's.
        logTimeMillis = Math.max(logTimeMillis, sc.timestampMillis());
        if (logTimeMillis >= nextSweepMillis) {
            sessions.expireIdleBefore(logTimeMillis - sessionTimeoutMillis);
            nextSweepMillis = logTimeMillis + EXPIRY_SWEEP_MILLIS;
        }
        return applySession(sc);
    }

    /**
     * The exactly-once rules (thesis §6.3):
     * <ul>
     *   <li>Unknown client with {@code ackedBelow == 1}: a new session. Nothing can have been
     *       forgotten yet, so it is safe to start one.</li>
     *   <li>Unknown client with {@code ackedBelow > 1}: the client has received replies, so we
     *       once had its session and have since expired it. We can no longer tell whether this
     *       request already ran, so we must not run it: SESSION_EXPIRED.</li>
     *   <li>{@code sequence < ackedBelow}: the client already has this reply (a stale duplicate).</li>
     *   <li>Result cached for this sequence: a retry. Return the cached result; do not execute.</li>
     *   <li>Otherwise: execute, cache, return.</li>
     * </ul>
     */
    private Bytes applySession(SessionCommand sc) {
        SessionTable.Session session = sessions.get(sc.clientId()).orElse(null);
        if (session == null) {
            if (sc.ackedBelow() > 1) {
                return outcome(ClientStatus.SESSION_EXPIRED, Bytes.EMPTY);
            }
            session = sessions.create(sc.clientId(), logTimeMillis);
        }
        session.lastActiveMillis = logTimeMillis;
        SessionTable.acknowledge(session, sc.ackedBelow());
        if (sc.sequence() < session.ackedBelow) {
            return outcome(ClientStatus.SESSION_EXPIRED, Bytes.EMPTY);
        }
        if (!bugs.contains(Bug.SESSION_DEDUP_DISABLED)) {
            Bytes cached = session.results.get(sc.sequence());
            if (cached != null) {
                return cached; // a retry: same answer, no second execution
            }
        }
        KVCommand command;
        try {
            command = KVCodec.decodeCommand(sc.command());
        } catch (CodecException e) {
            return outcome(ClientStatus.INVALID_REQUEST, Bytes.EMPTY);
        }
        Bytes result = outcome(ClientStatus.OK, KVCodec.encode(execute(command)));
        executed++;
        executionObserver.accept(sc.clientId(), sc.sequence());
        session.results.put(sc.sequence(), result);
        return result;
    }

    private KVResult execute(KVCommand command) {
        return switch (command) {
            case KVCommand.Put p -> new KVResult.PutResult(Optional.ofNullable(data.put(p.key(), p.value())));
            case KVCommand.Get g -> new KVResult.GetResult(Optional.ofNullable(data.get(g.key())));
            case KVCommand.Delete d -> new KVResult.DeleteResult(data.remove(d.key()) != null);
            case KVCommand.Cas c -> {
                Optional<Bytes> current = Optional.ofNullable(data.get(c.key()));
                if (current.equals(c.expected())) {
                    data.put(c.key(), c.newValue());
                    yield new KVResult.CasResult(true, Optional.of(c.newValue()));
                }
                yield new KVResult.CasResult(false, current);
            }
        };
    }

    private static Bytes outcome(ClientStatus status, Bytes result) {
        return KVCodec.encode(new KVCodec.Outcome(status, result));
    }

    // ------------------------------------------------------------------ inspection

    /** Current value of {@code key} on this replica (tests; not linearizable on its own). */
    public Optional<Bytes> peek(Bytes key) {
        return Optional.ofNullable(data.get(key));
    }

    public int size() {
        return data.size();
    }

    public int sessionCount() {
        return sessions.size();
    }

    /** How many commands were actually executed (retries answered from the cache don't count). */
    public long executedCommands() {
        return executed;
    }

    /** Called with (clientId, sequence) every time a command is really executed (tests, metrics). */
    public void setExecutionObserver(BiConsumer<UUID, Long> observer) {
        this.executionObserver = Objects.requireNonNull(observer, "observer");
    }

    /** Fingerprint of the full replicated state (data + sessions + logical time). */
    public long digest() {
        long h = Hashing.FNV64_OFFSET_BASIS;
        for (Map.Entry<Bytes, Bytes> e : data.entrySet()) {
            h = Hashing.fnv1a64(h, e.getKey().toArray());
            h = Hashing.fnv1a64(h, (byte) 0);
            h = Hashing.fnv1a64(h, e.getValue().toArray());
            h = Hashing.fnv1a64(h, (byte) 0);
        }
        h = SessionTable.mix(h, logTimeMillis);
        return sessions.digest(h);
    }
}
