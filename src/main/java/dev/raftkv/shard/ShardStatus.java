package dev.raftkv.shard;

/**
 * What a replica group is doing with one shard (SPEC §6.4).
 *
 * <pre>
 *              config makes it ours (prev owner != none)        InsertShard applied
 *  NOT_OWNED ───────────────────────────────────────▶ PULLING ─────────────────▶ GCING
 *      ▲  │   config makes it ours (fresh / was gid 0)                              │ DeleteShard acked
 *      │  └────────────────────────────────────────────────────────────▶ SERVING ◀──┘
 *      │  DeleteShard applied                config moves it away           │
 *      └──────────────────── BEPULLING ◀────────────────────────────────────┘
 * </pre>
 */
public enum ShardStatus {
    /** Not ours in the current configuration: WRONG_GROUP. */
    NOT_OWNED((byte) 0),
    /** Ours, data present: serving. */
    SERVING((byte) 1),
    /** Ours in the current configuration, data still at the previous owner: SHARD_NOT_READY. */
    PULLING((byte) 2),
    /** Ours in the previous configuration, not now; frozen data kept for the new owner: WRONG_GROUP. */
    BEPULLING((byte) 3),
    /** Ours, data installed, the previous owner not yet told to delete its copy: serving. */
    GCING((byte) 4);

    private final byte code;

    ShardStatus(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** Clients may use the shard (SERVING or GCING). */
    public boolean serves() {
        return this == SERVING || this == GCING;
    }

    public static ShardStatus fromCode(byte code) {
        for (ShardStatus s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        throw new dev.raftkv.codec.CodecException("unknown shard status " + code);
    }
}
