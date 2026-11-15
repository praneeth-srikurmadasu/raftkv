package dev.raftkv.protocol;

import dev.raftkv.codec.CodecException;

/**
 * Wire codes for message types. Codes are part of the protocol: never reuse or renumber one,
 * only add new ones (old nodes must be able to reject an unknown code cleanly).
 */
public enum MessageType {
    REQUEST_VOTE((short) 1),
    REQUEST_VOTE_REPLY((short) 2),
    APPEND_ENTRIES((short) 3),
    APPEND_ENTRIES_REPLY((short) 4),
    INSTALL_SNAPSHOT((short) 5),
    INSTALL_SNAPSHOT_REPLY((short) 6),
    TIMEOUT_NOW((short) 7),
    CLIENT_REQUEST((short) 100),
    CLIENT_REPLY((short) 101),
    FETCH_SHARD((short) 110),
    FETCH_SHARD_REPLY((short) 111),
    DELETE_SHARD((short) 112),
    DELETE_SHARD_REPLY((short) 113);

    private final short code;

    MessageType(short code) {
        this.code = code;
    }

    public short code() {
        return code;
    }

    public static MessageType fromCode(short code) {
        for (MessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new CodecException("unknown message type " + code);
    }
}
