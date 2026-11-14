package dev.raftkv.codec;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * Encodes a {@link Frame} into bytes.
 *
 * <pre>
 *   offset 0   length        int    number of bytes that follow this field
 *          4   type          short
 *          6   groupId       int
 *         10   correlationId long
 *         18   flags         byte
 *         19   payload       N bytes
 *       19+N   crc32c        int    CRC32C of bytes [0, 19+N), i.e. everything before it
 * </pre>
 *
 * <p>The length prefix is how a receiver finds frame boundaries in a TCP byte stream (TCP
 * delivers bytes, not messages; one read can return half a frame or three frames). The CRC
 * covers the length field too, so a corrupted length is caught once the frame is complete.
 * TCP has its own checksum, but it is only 16 bits and misses real-world corruption; the same
 * format will protect our on-disk log records.
 */
public final class FrameCodec {

    /** Size of the length field. */
    public static final int LENGTH_FIELD_SIZE = 4;
    /** type + groupId + correlationId + flags. */
    public static final int HEADER_SIZE = 2 + 4 + 8 + 1;
    /** CRC32C. */
    public static final int TRAILER_SIZE = 4;
    /** Smallest legal value of the length field (empty payload). */
    public static final int MIN_LENGTH = HEADER_SIZE + TRAILER_SIZE;
    /** Default cap on payload size; bigger frames are rejected before any allocation. */
    public static final int DEFAULT_MAX_PAYLOAD = 16 * 1024 * 1024;

    private FrameCodec() {
    }

    /** Returns a buffer positioned at 0 containing exactly one encoded frame. */
    public static ByteBuffer encode(Frame frame) {
        int payloadLength = frame.payload().length();
        int length = HEADER_SIZE + payloadLength + TRAILER_SIZE;
        ByteBuffer buf = ByteBuffer.allocate(LENGTH_FIELD_SIZE + length);
        buf.putInt(length);
        buf.putShort(frame.type());
        buf.putInt(frame.groupId());
        buf.putLong(frame.correlationId());
        buf.put(frame.flags());
        frame.payload().writeTo(buf);
        buf.putInt((int) crc(buf.array(), 0, buf.position()));
        buf.flip();
        return buf;
    }

    /** Convenience: encoded frame as a byte array. */
    public static byte[] encodeToArray(Frame frame) {
        return encode(frame).array();
    }

    static long crc(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return crc.getValue();
    }
}
