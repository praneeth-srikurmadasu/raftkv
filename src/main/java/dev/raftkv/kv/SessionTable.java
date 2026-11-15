package dev.raftkv.kv;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.Hashing;

/**
 * Per-client state for exactly-once semantics (Raft §8, thesis §6.3, RIFL).
 *
 * <p>For each client: the results of requests it has not yet acknowledged, keyed by sequence
 * number, and its acknowledgement watermark. A request whose result is cached is a retry and
 * gets the cached result without being executed again. Results below the watermark are
 * dropped, because the client promised never to ask for them again.
 *
 * <p>This is part of the replicated state, so it must be deterministic: sorted maps only, and
 * time comes from log entries, never from a local clock. The single-group KV store keeps one
 * table; the sharded store keeps one per shard, so the dedup state of a key moves with its shard.
 */
public final class SessionTable {

    private static final class Session {
        long ackedBelow = 1;
        long lastActiveMillis;
        final NavigableMap<Long, Bytes> results = new TreeMap<>();
    }

    /** What to do with an incoming request. */
    public enum Verdict {
        /** Not seen before: execute it, then {@link #recordResult}. */
        EXECUTE,
        /** A retry of a request that already ran: answer with {@link Admission#cached()}. */
        CACHED,
        /** We can't tell whether it ran (session forgotten, or already acknowledged): don't run it. */
        EXPIRED
    }

    /** @param cached the earlier result, for {@link Verdict#CACHED} */
    public record Admission(Verdict verdict, Bytes cached) {
    }

    private final TreeMap<UUID, Session> sessions = new TreeMap<>();

    /**
     * Applies the session rules to one request (at apply time, in log order):
     * <ul>
     *   <li>Unknown client: a new session; except with {@code strictNewSessions}, where an unknown
     *       client with {@code ackedBelow > 1} must once have had a session that has since
     *       expired, so the request may already have run: EXPIRED.</li>
     *   <li>{@code sequence <} the watermark: the client already has this reply: EXPIRED.</li>
     *   <li>Result cached for this sequence (and {@code dedup}): CACHED.</li>
     *   <li>Otherwise: EXECUTE.</li>
     * </ul>
     */
    public Admission admit(UUID clientId, long sequence, long ackedBelow, long nowMillis,
                           boolean strictNewSessions, boolean dedup) {
        Session s = sessions.get(clientId);
        if (s == null) {
            if (strictNewSessions && ackedBelow > 1) {
                return new Admission(Verdict.EXPIRED, null);
            }
            s = new Session();
            sessions.put(clientId, s);
        }
        s.lastActiveMillis = nowMillis;
        if (ackedBelow > s.ackedBelow) {
            s.ackedBelow = ackedBelow;
            s.results.headMap(ackedBelow, false).clear();
        }
        if (sequence < s.ackedBelow) {
            return new Admission(Verdict.EXPIRED, null);
        }
        if (dedup) {
            Bytes cached = s.results.get(sequence);
            if (cached != null) {
                return new Admission(Verdict.CACHED, cached);
            }
        }
        return new Admission(Verdict.EXECUTE, null);
    }

    /** Caches the result of a request that was just executed after {@link #admit} said EXECUTE. */
    public void recordResult(UUID clientId, long sequence, Bytes result) {
        Session s = Objects.requireNonNull(sessions.get(clientId), "no session for " + clientId);
        s.results.put(sequence, result);
    }

    /** Removes sessions idle since before {@code cutoffMillis}; returns how many. */
    public int expireIdleBefore(long cutoffMillis) {
        int removed = 0;
        Iterator<Map.Entry<UUID, Session>> it = sessions.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().lastActiveMillis < cutoffMillis) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    public int size() {
        return sessions.size();
    }

    /** A fingerprint of the whole table (replicas must agree on it). */
    public long digest(long hash) {
        long h = hash;
        for (Map.Entry<UUID, Session> e : sessions.entrySet()) {
            h = mix(h, e.getKey().getMostSignificantBits());
            h = mix(h, e.getKey().getLeastSignificantBits());
            Session s = e.getValue();
            h = mix(h, s.ackedBelow);
            h = mix(h, s.lastActiveMillis);
            for (Map.Entry<Long, Bytes> r : s.results.entrySet()) {
                h = mix(h, r.getKey());
                h = Hashing.fnv1a64(h, r.getValue().toArray());
            }
        }
        return h;
    }

    public static long mix(long h, long v) {
        long x = h;
        for (int i = 0; i < 8; i++) {
            x = Hashing.fnv1a64(x, (byte) (v >>> (8 * i)));
        }
        return x;
    }

    // ------------------------------------------------------------------ serialization (snapshots, shard moves)

    public void writeTo(BinaryWriter w) {
        w.writeInt(sessions.size());
        for (Map.Entry<UUID, Session> e : sessions.entrySet()) {
            Session s = e.getValue();
            w.writeLong(e.getKey().getMostSignificantBits());
            w.writeLong(e.getKey().getLeastSignificantBits());
            w.writeLong(s.ackedBelow);
            w.writeLong(s.lastActiveMillis);
            w.writeInt(s.results.size());
            for (Map.Entry<Long, Bytes> r : s.results.entrySet()) {
                w.writeLong(r.getKey());
                w.writeBytes(r.getValue());
            }
        }
    }

    public static SessionTable readFrom(BinaryReader r) {
        SessionTable t = new SessionTable();
        int count = readCount(r, 36);
        for (int i = 0; i < count; i++) {
            UUID id = new UUID(r.readLong(), r.readLong());
            Session s = new Session();
            s.ackedBelow = r.readLong();
            s.lastActiveMillis = r.readLong();
            int results = readCount(r, 12);
            for (int j = 0; j < results; j++) {
                s.results.put(r.readLong(), r.readBytes());
            }
            t.sessions.put(id, s);
        }
        return t;
    }

    /** A deep copy (shard data handed to another group must not share mutable state). */
    public SessionTable copy() {
        BinaryWriter w = new BinaryWriter();
        writeTo(w);
        return readFrom(new BinaryReader(w.toBytes()));
    }

    /** Reads an element count and checks it against the bytes left (each element needs at least {@code minBytes}). */
    public static int readCount(BinaryReader r, int minBytes) {
        int count = r.readInt();
        if (count < 0 || (long) count * minBytes > r.remaining()) {
            throw new CodecException("invalid element count " + count);
        }
        return count;
    }
}
