package dev.raftkv.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.raftkv.common.Bytes;

/**
 * Turns a stream of byte chunks (as TCP delivers them) back into {@link Frame}s.
 *
 * <p>Feed it whatever bytes arrived; it returns every frame completed so far and keeps the
 * leftover partial frame for the next call. One decoder per connection; not thread-safe.
 *
 * <p>After a {@link CodecException} the stream is out of sync (we no longer know where the
 * next frame starts), so the decoder refuses further input and the connection must be closed.
 */
public final class FrameDecoder {

    private final int maxPayload;
    private byte[] pending = new byte[256];
    private int pendingLength;
    private boolean failed;

    public FrameDecoder() {
        this(FrameCodec.DEFAULT_MAX_PAYLOAD);
    }

    public FrameDecoder(int maxPayload) {
        if (maxPayload < 0) {
            throw new IllegalArgumentException("maxPayload must be >= 0");
        }
        this.maxPayload = maxPayload;
    }

    /** Consumes all remaining bytes of {@code chunk} and returns the frames they completed. */
    public List<Frame> feed(ByteBuffer chunk) {
        if (failed) {
            throw new IllegalStateException("decoder failed earlier; the connection must be closed");
        }
        append(chunk);
        List<Frame> frames = new ArrayList<>();
        try {
            int offset = 0;
            while (true) {
                int available = pendingLength - offset;
                if (available < FrameCodec.LENGTH_FIELD_SIZE) {
                    break;
                }
                int length = ByteBuffer.wrap(pending, offset, FrameCodec.LENGTH_FIELD_SIZE).getInt();
                // Validate the length BEFORE waiting for / allocating that many bytes.
                if (length < FrameCodec.MIN_LENGTH || length - FrameCodec.MIN_LENGTH > maxPayload) {
                    throw new CodecException("invalid frame length " + length
                            + " (allowed " + FrameCodec.MIN_LENGTH + ".." + (FrameCodec.MIN_LENGTH + (long) maxPayload) + ")");
                }
                int total = FrameCodec.LENGTH_FIELD_SIZE + length;
                if (available < total) {
                    break;
                }
                frames.add(parse(offset, total));
                offset += total;
            }
            compact(offset);
        } catch (CodecException e) {
            failed = true;
            throw e;
        }
        return frames;
    }

    /** Convenience for tests and simple callers. */
    public List<Frame> feed(byte[] chunk) {
        return feed(ByteBuffer.wrap(chunk));
    }

    /** Bytes received that do not yet form a complete frame. */
    public int bufferedBytes() {
        return pendingLength;
    }

    private Frame parse(int offset, int total) {
        int crcOffset = offset + total - FrameCodec.TRAILER_SIZE;
        int expected = ByteBuffer.wrap(pending, crcOffset, FrameCodec.TRAILER_SIZE).getInt();
        int actual = (int) FrameCodec.crc(pending, offset, total - FrameCodec.TRAILER_SIZE);
        if (expected != actual) {
            throw new CodecException(String.format("frame CRC mismatch: expected %08x, computed %08x", expected, actual));
        }
        ByteBuffer header = ByteBuffer.wrap(pending, offset + FrameCodec.LENGTH_FIELD_SIZE, FrameCodec.HEADER_SIZE);
        short type = header.getShort();
        int groupId = header.getInt();
        long correlationId = header.getLong();
        byte flags = header.get();
        if (groupId < 0) {
            throw new CodecException("negative group id " + groupId);
        }
        int payloadOffset = offset + FrameCodec.LENGTH_FIELD_SIZE + FrameCodec.HEADER_SIZE;
        int payloadLength = total - FrameCodec.LENGTH_FIELD_SIZE - FrameCodec.HEADER_SIZE - FrameCodec.TRAILER_SIZE;
        return new Frame(type, groupId, correlationId, flags, Bytes.copyOf(pending, payloadOffset, payloadLength));
    }

    private void append(ByteBuffer chunk) {
        int n = chunk.remaining();
        if (pending.length - pendingLength < n) {
            long needed = (long) pendingLength + n;
            if (needed > Integer.MAX_VALUE - 8) {
                throw new IllegalStateException("decoder buffer overflow");
            }
            int capacity = (int) Math.min(Math.max(needed, (long) pending.length * 2), Integer.MAX_VALUE - 8);
            pending = java.util.Arrays.copyOf(pending, capacity);
        }
        chunk.get(pending, pendingLength, n);
        pendingLength += n;
    }

    private void compact(int consumed) {
        if (consumed == 0) {
            return;
        }
        System.arraycopy(pending, consumed, pending, 0, pendingLength - consumed);
        pendingLength -= consumed;
    }
}
