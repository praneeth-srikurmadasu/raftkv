package dev.raftkv.raft.log;

import dev.raftkv.codec.CodecException;

/** What a log entry carries. The code is the on-wire / on-disk byte. */
public enum EntryType {
    /** A state-machine command (e.g. a KV PUT). */
    COMMAND((byte) 1),
    /** Empty entry a new leader appends to commit entries from earlier terms (Raft §5.4.2, §8). */
    NOOP((byte) 2),
    /** A cluster membership configuration (Raft thesis ch. 4). */
    CONFIGURATION((byte) 3);

    private final byte code;

    EntryType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static EntryType fromCode(byte code) {
        for (EntryType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new CodecException("unknown entry type " + code);
    }
}
