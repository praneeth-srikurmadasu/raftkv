package dev.raftkv.codec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import dev.raftkv.common.Bytes;

/**
 * Hand-written binary serializer. Big-endian (network byte order), fixed-width integers,
 * length-prefixed variable-size fields.
 *
 * <p>Why not Java serialization ({@code ObjectOutputStream})? It is slow and verbose, ties the
 * wire format to class internals (renaming a field breaks compatibility), and deserializing
 * untrusted bytes has a long history of remote-code-execution vulnerabilities. Our format
 * is explicit: every byte is written by code you can read.
 *
 * <p>The buffer grows automatically (doubling), so callers never size it up front.
 */
public final class BinaryWriter {

    private ByteBuffer buffer;

    public BinaryWriter() {
        this(64);
    }

    public BinaryWriter(int initialCapacity) {
        if (initialCapacity < 1) {
            throw new IllegalArgumentException("initial capacity must be >= 1");
        }
        this.buffer = ByteBuffer.allocate(initialCapacity); // big-endian by default
    }

    public BinaryWriter writeByte(int value) {
        ensure(1);
        buffer.put((byte) value);
        return this;
    }

    /** One byte: 1 for true, 0 for false. */
    public BinaryWriter writeBoolean(boolean value) {
        return writeByte(value ? 1 : 0);
    }

    public BinaryWriter writeShort(int value) {
        ensure(2);
        buffer.putShort((short) value);
        return this;
    }

    public BinaryWriter writeInt(int value) {
        ensure(4);
        buffer.putInt(value);
        return this;
    }

    public BinaryWriter writeLong(long value) {
        ensure(8);
        buffer.putLong(value);
        return this;
    }

    /** {@code [length:4][bytes]}. */
    public BinaryWriter writeBytes(Bytes value) {
        writeInt(value.length());
        ensure(value.length());
        value.writeTo(buffer);
        return this;
    }

    /**
     * {@code [length:4][UTF-8 bytes]}. Rejects strings that are not valid Unicode (an unpaired
     * surrogate cannot be encoded as UTF-8; silently replacing it with '?' would mean the
     * receiver sees a different string than the sender wrote).
     */
    public BinaryWriter writeString(String value) {
        ByteBuffer encoded;
        try {
            CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            encoded = encoder.encode(CharBuffer.wrap(value));
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("string is not valid Unicode (unpaired surrogate?)", e);
        }
        writeInt(encoded.remaining());
        ensure(encoded.remaining());
        buffer.put(encoded);
        return this;
    }

    /** Number of bytes written so far. */
    public int size() {
        return buffer.position();
    }

    /** A copy of everything written so far. */
    public byte[] toByteArray() {
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    /** Everything written so far, as immutable {@link Bytes}. */
    public Bytes toBytes() {
        return Bytes.copyOf(buffer.array(), 0, buffer.position());
    }

    private void ensure(int extra) {
        if (buffer.remaining() >= extra) {
            return;
        }
        long needed = (long) buffer.position() + extra;
        if (needed > Integer.MAX_VALUE - 8) {
            throw new IllegalStateException("message too large: " + needed + " bytes");
        }
        long newCapacity = Math.max(needed, (long) buffer.capacity() * 2);
        int capacity = (int) Math.min(newCapacity, Integer.MAX_VALUE - 8);
        ByteBuffer bigger = ByteBuffer.allocate(capacity);
        buffer.flip();
        bigger.put(buffer);
        buffer = bigger;
    }
}
