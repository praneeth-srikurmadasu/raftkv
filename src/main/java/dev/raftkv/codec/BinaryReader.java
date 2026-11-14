package dev.raftkv.codec;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import dev.raftkv.common.Bytes;

/**
 * Reads what {@link BinaryWriter} wrote.
 *
 * <p>Every read checks that enough bytes remain and throws {@link CodecException} otherwise,
 * so truncated or hostile input can never cause a {@code BufferUnderflowException}, a negative
 * array size, or a giant allocation: a length prefix is only trusted after checking it
 * against the bytes actually present.
 */
public final class BinaryReader {

    private final ByteBuffer buffer;

    public BinaryReader(byte[] data) {
        this.buffer = ByteBuffer.wrap(data); // big-endian, matches BinaryWriter
    }

    public BinaryReader(Bytes data) {
        this(data.toArray());
    }

    public byte readByte() {
        require(1, "byte");
        return buffer.get();
    }

    /** Accepts exactly 0 or 1; anything else means the data is corrupt. */
    public boolean readBoolean() {
        int offset = buffer.position();
        byte b = readByte();
        return switch (b) {
            case 0 -> false;
            case 1 -> true;
            default -> throw new CodecException("invalid boolean " + b + " at offset " + offset);
        };
    }

    public short readShort() {
        require(2, "short");
        return buffer.getShort();
    }

    public int readInt() {
        require(4, "int");
        return buffer.getInt();
    }

    public long readLong() {
        require(8, "long");
        return buffer.getLong();
    }

    /** Reads {@code [length:4][bytes]}. */
    public Bytes readBytes() {
        int length = readLength("bytes");
        byte[] out = new byte[length];
        buffer.get(out);
        return Bytes.copyOf(out);
    }

    /** Reads {@code [length:4][UTF-8 bytes]}; malformed UTF-8 is rejected, not replaced. */
    public String readString() {
        int length = readLength("string");
        ByteBuffer slice = buffer.slice(buffer.position(), length);
        buffer.position(buffer.position() + length);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(slice)
                    .toString();
        } catch (CharacterCodingException e) {
            throw new CodecException("string is not valid UTF-8");
        }
    }

    /** Bytes not yet read. */
    public int remaining() {
        return buffer.remaining();
    }

    /** Current read offset. */
    public int position() {
        return buffer.position();
    }

    /**
     * Call after decoding a complete message: leftover bytes mean the sender and receiver
     * disagree about the format, which must be an error, never silently ignored.
     */
    public void expectEnd() {
        if (buffer.hasRemaining()) {
            throw new CodecException(buffer.remaining() + " unexpected trailing bytes at offset " + buffer.position());
        }
    }

    private int readLength(String what) {
        int offset = buffer.position();
        int length = readInt();
        if (length < 0) {
            throw new CodecException("negative " + what + " length " + length + " at offset " + offset);
        }
        if (length > buffer.remaining()) {
            throw new CodecException(what + " length " + length + " at offset " + offset
                    + " exceeds remaining " + buffer.remaining() + " bytes");
        }
        return length;
    }

    private void require(int n, String what) {
        if (buffer.remaining() < n) {
            throw new CodecException("need " + n + " bytes for " + what + " at offset " + buffer.position()
                    + ", only " + buffer.remaining() + " left");
        }
    }
}
