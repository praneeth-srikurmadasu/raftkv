package dev.raftkv.kv;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

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
 * time comes from log entries, never from a local clock.
 */
final class SessionTable {

    static final class Session {
        long ackedBelow = 1;
        long lastActiveMillis;
        final NavigableMap<Long, Bytes> results = new TreeMap<>();
    }

    private final TreeMap<UUID, Session> sessions = new TreeMap<>();

    Optional<Session> get(UUID clientId) {
        return Optional.ofNullable(sessions.get(clientId));
    }

    Session create(UUID clientId, long nowMillis) {
        Session s = new Session();
        s.lastActiveMillis = nowMillis;
        sessions.put(clientId, s);
        return s;
    }

    /** Raises the watermark and forgets results the client has acknowledged. */
    static void acknowledge(Session s, long ackedBelow) {
        if (ackedBelow > s.ackedBelow) {
            s.ackedBelow = ackedBelow;
            s.results.headMap(ackedBelow, false).clear();
        }
    }

    /** Removes sessions idle since before {@code cutoffMillis}; returns how many. */
    int expireIdleBefore(long cutoffMillis) {
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

    int size() {
        return sessions.size();
    }

    /** A fingerprint of the whole table (replicas must agree on it). */
    long digest(long hash) {
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

    static long mix(long h, long v) {
        long x = h;
        for (int i = 0; i < 8; i++) {
            x = Hashing.fnv1a64(x, (byte) (v >>> (8 * i)));
        }
        return x;
    }
}
