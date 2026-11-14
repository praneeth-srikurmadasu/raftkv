package dev.raftkv.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Random;

import dev.raftkv.common.Bytes;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

class BinaryCodecTest {

    /** Property: whatever we write, we read back exactly, and nothing is left over. */
    @Property
    void primitivesRoundTrip(@ForAll byte b, @ForAll short s, @ForAll int i, @ForAll long l,
                             @ForAll boolean z, @ForAll byte[] data) {
        byte[] encoded = new BinaryWriter(1) // capacity 1 forces the buffer to grow many times
                .writeByte(b).writeShort(s).writeInt(i).writeLong(l).writeBoolean(z)
                .writeBytes(Bytes.copyOf(data))
                .toByteArray();

        BinaryReader r = new BinaryReader(encoded);
        assertThat(r.readByte()).isEqualTo(b);
        assertThat(r.readShort()).isEqualTo(s);
        assertThat(r.readInt()).isEqualTo(i);
        assertThat(r.readLong()).isEqualTo(l);
        assertThat(r.readBoolean()).isEqualTo(z);
        assertThat(r.readBytes()).isEqualTo(Bytes.copyOf(data));
        r.expectEnd();
    }

    @Test
    void usesBigEndianFixedWidthLayout() {
        byte[] encoded = new BinaryWriter().writeShort(0x0102).writeInt(0x03040506).toByteArray();
        assertThat(Bytes.copyOf(encoded)).isEqualTo(Bytes.copyOf(new byte[] {1, 2, 3, 4, 5, 6}));
        byte[] bytesField = new BinaryWriter().writeBytes(Bytes.copyOf(new byte[] {9})).toByteArray();
        assertThat(Bytes.copyOf(bytesField)).isEqualTo(Bytes.copyOf(new byte[] {0, 0, 0, 1, 9})); // [length:4][data]
    }

    @Test
    void stringsRoundTripIncludingMultiByteCharacters() {
        List<String> samples = List.of("", "raft", "héllo", "☃ snow", "𝄞 clef (4-byte UTF-8)", "日本語");
        for (String s : samples) {
            assertThat(roundTrip(s)).isEqualTo(s);
        }
        // Random valid strings: any code point except the surrogate range.
        Random random = new Random(42);
        for (int n = 0; n < 1000; n++) {
            StringBuilder sb = new StringBuilder();
            int len = random.nextInt(20);
            for (int k = 0; k < len; k++) {
                int cp;
                do {
                    cp = random.nextInt(Character.MAX_CODE_POINT + 1);
                } while (cp >= Character.MIN_SURROGATE && cp <= Character.MAX_SURROGATE);
                sb.appendCodePoint(cp);
            }
            String s = sb.toString();
            assertThat(roundTrip(s)).isEqualTo(s);
        }
    }

    @Test
    void refusesToWriteInvalidUnicode() {
        String unpairedSurrogate = "a\uD800b";
        assertThatThrownBy(() -> new BinaryWriter().writeString(unpairedSurrogate))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMalformedUtf8OnRead() {
        byte[] encoded = {0, 0, 0, 2, (byte) 0xC3, 0x28}; // 0xC3 must be followed by a continuation byte
        assertThatThrownBy(() -> new BinaryReader(encoded).readString())
                .isInstanceOf(CodecException.class)
                .hasMessageContaining("UTF-8");
    }

    @Test
    void truncatedInputThrowsCodecExceptionNotUnderflow() {
        byte[] three = {1, 2, 3};
        assertThatThrownBy(() -> new BinaryReader(three).readInt()).isInstanceOf(CodecException.class);
        assertThatThrownBy(() -> new BinaryReader(three).readLong()).isInstanceOf(CodecException.class);
        assertThatThrownBy(() -> new BinaryReader(new byte[1]).readShort()).isInstanceOf(CodecException.class);
        assertThatThrownBy(() -> new BinaryReader(new byte[0]).readByte()).isInstanceOf(CodecException.class);
    }

    @Test
    void lengthPrefixIsCheckedBeforeAllocating() {
        byte[] negative = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThatThrownBy(() -> new BinaryReader(negative).readBytes())
                .isInstanceOf(CodecException.class).hasMessageContaining("negative");

        // Claims 2 GB but only 1 byte follows: must fail fast, not try to allocate 2 GB.
        byte[] huge = {0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 42};
        assertThatThrownBy(() -> new BinaryReader(huge).readBytes())
                .isInstanceOf(CodecException.class).hasMessageContaining("exceeds remaining");
    }

    @Test
    void booleansMustBeZeroOrOne() {
        assertThatThrownBy(() -> new BinaryReader(new byte[] {2}).readBoolean())
                .isInstanceOf(CodecException.class).hasMessageContaining("invalid boolean");
    }

    @Test
    void trailingBytesAreAnError() {
        BinaryReader r = new BinaryReader(new byte[] {0, 0, 0, 7, 99});
        assertThat(r.readInt()).isEqualTo(7);
        assertThat(r.remaining()).isEqualTo(1);
        assertThatThrownBy(r::expectEnd).isInstanceOf(CodecException.class).hasMessageContaining("trailing");
    }

    private static String roundTrip(String s) {
        BinaryReader r = new BinaryReader(new BinaryWriter().writeString(s).toByteArray());
        String out = r.readString();
        r.expectEnd();
        return out;
    }
}
