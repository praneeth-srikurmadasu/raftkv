package dev.raftkv.codec;

import java.util.Objects;

import dev.raftkv.common.Bytes;

/**
 * One unit on the wire: a header plus an opaque payload (SPEC §5.1).
 *
 * <pre>
 *   [length:4][type:2][groupId:4][correlationId:8][flags:1][payload:N][crc32c:4]
 * </pre>
 *
 * The codec layer does not know what the payload means; {@code dev.raftkv.protocol} maps
 * {@code type} to a message class.
 *
 * @param type          message type code
 * @param groupId       Raft group this frame belongs to (lets one connection carry many groups)
 * @param correlationId matches a response to its request (0 when unused)
 * @param flags         reserved bits for future use (compression, priority...)
 * @param payload       encoded message body
 */
public record Frame(short type, int groupId, long correlationId, byte flags, Bytes payload) {

    public Frame {
        Objects.requireNonNull(payload, "payload");
        if (groupId < 0) {
            throw new IllegalArgumentException("groupId must be >= 0, was " + groupId);
        }
    }
}
