package dev.raftkv.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BytesTest {

    @Test
    void copiesOnTheWayInAndOut() {
        byte[] source = {1, 2, 3};
        Bytes b = Bytes.copyOf(source);
        source[0] = 99;                       // changing the source must not change b
        assertThat(b.get(0)).isEqualTo((byte) 1);

        byte[] out = b.toArray();
        out[1] = 99;                          // changing the output must not change b
        assertThat(b.get(1)).isEqualTo((byte) 2);
    }

    @Test
    void equalityAndHashCodeUseContents() {
        Bytes a = Bytes.copyOf(new byte[] {1, 2, 3});
        Bytes b = Bytes.copyOf(new byte[] {1, 2, 3});
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(Bytes.copyOf(new byte[] {1, 2, 4}));
        assertThat(Bytes.copyOf(new byte[0])).isSameAs(Bytes.EMPTY);
    }

    @Test
    void ordersBytesAsUnsigned() {
        Bytes small = Bytes.copyOf(new byte[] {0x7f});
        Bytes big = Bytes.copyOf(new byte[] {(byte) 0x80}); // 128 unsigned, -128 signed
        assertThat(small.compareTo(big)).isLessThan(0);
        assertThat(Bytes.EMPTY.compareTo(Bytes.copyOf(new byte[] {0}))).isLessThan(0);
        assertThat(Bytes.copyOf(new byte[] {1}).compareTo(Bytes.copyOf(new byte[] {1, 0}))).isLessThan(0);
    }

    @Test
    void utf8RoundTrip() {
        String text = "héllo ☃ 𝄞";
        assertThat(Bytes.ofUtf8(text).toUtf8()).isEqualTo(text);
    }

    @Test
    void rangeCopyIsBoundsChecked() {
        byte[] source = {1, 2, 3, 4};
        assertThat(Bytes.copyOf(source, 1, 2)).isEqualTo(Bytes.copyOf(new byte[] {2, 3}));
        assertThatThrownBy(() -> Bytes.copyOf(source, 3, 2)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void toStringShowsLengthAndTruncatedHex() {
        assertThat(Bytes.copyOf(new byte[] {10, 11}).toString()).isEqualTo("Bytes[2:0a0b]");
        assertThat(Bytes.copyOf(new byte[20]).toString()).isEqualTo("Bytes[20:" + "00".repeat(16) + "...]");
    }
}
