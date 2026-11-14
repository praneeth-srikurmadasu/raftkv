package dev.raftkv.protocol;

import dev.raftkv.codec.CodecException;

/**
 * Outcome of a client request (SPEC §4.5). The client's retry policy depends on it.
 */
public enum ClientStatus {
    /** Done; the result field holds the operation's result. */
    OK((byte) 0),
    /** This server is not the leader (or lost leadership): retry at {@code leaderHint}, same sequence number. */
    NOT_LEADER((byte) 1),
    /** The server no longer has this client's session; the outcome of this request is unknown. */
    SESSION_EXPIRED((byte) 2),
    /** Malformed request (bad command bytes, key or value too large). Retrying won't help. */
    INVALID_REQUEST((byte) 3),
    /** The key's shard is owned by another group (Milestone 10). */
    WRONG_GROUP((byte) 4),
    /** The key's shard is still being moved into this group (Milestone 10): retry here later. */
    SHARD_NOT_READY((byte) 5),
    /** The server is shedding load: back off and retry. */
    OVERLOADED((byte) 6);

    private final byte code;

    ClientStatus(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static ClientStatus fromCode(byte code) {
        for (ClientStatus s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        throw new CodecException("unknown client status " + code);
    }
}
