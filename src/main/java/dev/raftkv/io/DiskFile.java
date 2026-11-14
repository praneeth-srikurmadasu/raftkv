package dev.raftkv.io;

import java.nio.ByteBuffer;

/** An open file with positional reads and writes. Not thread-safe. */
public interface DiskFile extends AutoCloseable {

    /** Current size in bytes (including writes not yet forced). */
    long size();

    /** Reads up to {@code dst.remaining()} bytes starting at {@code position}; returns the count (0 at end of file). */
    int read(ByteBuffer dst, long position);

    /** Writes all remaining bytes of {@code src} at {@code position}, extending the file if needed. */
    void write(ByteBuffer src, long position);

    /** Shrinks the file to {@code size} bytes (no-op if already smaller). */
    void truncate(long size);

    /** Makes all writes and truncations so far durable (fsync / fdatasync). */
    void force();

    @Override
    void close();

    /** Reads exactly {@code length} bytes at {@code position} or throws if the file is shorter. */
    default byte[] readFully(long position, int length) {
        ByteBuffer buf = ByteBuffer.allocate(length);
        long pos = position;
        while (buf.hasRemaining()) {
            int n = read(buf, pos);
            if (n <= 0) {
                throw new DiskException("unexpected end of file at " + pos + " (wanted " + length + " bytes at " + position + ")");
            }
            pos += n;
        }
        return buf.array();
    }
}
