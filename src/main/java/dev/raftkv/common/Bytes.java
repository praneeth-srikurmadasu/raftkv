package dev.raftkv.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/**
 * An immutable sequence of bytes with value semantics.
 *
 * <p>Why not just {@code byte[]}? Two reasons:
 * <ol>
 *   <li><b>Equality.</b> Java arrays use identity for {@code equals}/{@code hashCode}. A
 *       {@code record LogEntry(byte[] data)} would therefore never equal a decoded copy of
 *       itself, and round-trip tests would silently compare references. {@code Bytes}
 *       compares contents, so every record that holds it gets correct equality for free.</li>
 *   <li><b>Immutability.</b> Arrays can be modified by anyone holding them. A log entry
 *       must never change after it is created, so {@code Bytes} copies on the way in and
 *       on the way out.</li>
 * </ol>
 *
 * <p>Ordering ({@link #compareTo}) is lexicographic over <em>unsigned</em> bytes, the same
 * order every byte-oriented key-value store uses. The KV layer will rely on it.
 */
public final class Bytes implements Comparable<Bytes> {

    public static final Bytes EMPTY = new Bytes(new byte[0]);

    private static final int TO_STRING_MAX_BYTES = 16;

    private final byte[] data;

    private Bytes(byte[] data) {
        this.data = data;
    }

    /** Copies {@code source}; later changes to the array do not affect the result. */
    public static Bytes copyOf(byte[] source) {
        Objects.requireNonNull(source, "source");
        return source.length == 0 ? EMPTY : new Bytes(source.clone());
    }

    /** Copies {@code length} bytes of {@code source} starting at {@code offset}. */
    public static Bytes copyOf(byte[] source, int offset, int length) {
        Objects.requireNonNull(source, "source");
        Objects.checkFromIndexSize(offset, length, source.length);
        return length == 0 ? EMPTY : new Bytes(Arrays.copyOfRange(source, offset, offset + length));
    }

    /** The UTF-8 encoding of {@code text}. */
    public static Bytes ofUtf8(String text) {
        return copyOf(text.getBytes(StandardCharsets.UTF_8));
    }

    public int length() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    public byte get(int index) {
        return data[index];
    }

    /** A fresh copy of the contents. */
    public byte[] toArray() {
        return data.clone();
    }

    /** Writes the contents into {@code buffer} without an intermediate copy. */
    public void writeTo(ByteBuffer buffer) {
        buffer.put(data);
    }

    /** Decodes the contents as UTF-8 (malformed input becomes U+FFFD). */
    public String toUtf8() {
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(Bytes other) {
        return Arrays.compareUnsigned(data, other.data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Bytes other && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    /** Short hex form, e.g. {@code Bytes[3:0a0b0c]}; long values are truncated with "...". */
    @Override
    public String toString() {
        int shown = Math.min(data.length, TO_STRING_MAX_BYTES);
        String hex = HexFormat.of().formatHex(data, 0, shown);
        return "Bytes[" + data.length + ":" + hex + (data.length > shown ? "..." : "") + "]";
    }
}
