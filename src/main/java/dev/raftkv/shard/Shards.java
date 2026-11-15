package dev.raftkv.shard;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.Hashing;

/**
 * Key → shard mapping (SPEC §6.1): {@code shard = hash64(key) mod N}, with the hand-written
 * FNV-1a 64 from {@link Hashing}. Never {@code Arrays.hashCode}/{@code Object.hashCode}: neither is
 * a stable contract across JVM versions, and every client and server must agree forever.
 */
public final class Shards {

    /** Shards in a real cluster (constant for the cluster's lifetime). */
    public static final int DEFAULT_COUNT = 64;

    private Shards() {
    }

    public static int shardOf(Bytes key, int numShards) {
        return (int) Long.remainderUnsigned(Hashing.fnv1a64(key.toArray()), numShards);
    }
}
