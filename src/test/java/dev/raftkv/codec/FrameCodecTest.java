package dev.raftkv.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import dev.raftkv.common.Bytes;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

class FrameCodecTest {

    @Test
    void roundTripsOneFrame() {
        Frame frame = new Frame((short) 3, 7, 123456789L, (byte) 1, Bytes.ofUtf8("payload"));
        List<Frame> decoded = new FrameDecoder().feed(FrameCodec.encodeToArray(frame));
        assertThat(decoded).isEqualTo(List.of(frame));
    }

    @Test
    void layoutMatchesTheSpec() {
        Frame frame = new Frame((short) 0x0102, 0x03040506, 0x0708090A0B0C0D0EL, (byte) 0x0F, Bytes.copyOf(new byte[] {0x10, 0x11}));
        byte[] wire = FrameCodec.encodeToArray(frame);
        assertThat(wire.length).isEqualTo(4 + 15 + 2 + 4);
        ByteBuffer b = ByteBuffer.wrap(wire);
        assertThat(b.getInt()).isEqualTo(15 + 2 + 4);     // length = bytes after the length field
        assertThat(b.getShort()).isEqualTo((short) 0x0102);
        assertThat(b.getInt()).isEqualTo(0x03040506);
        assertThat(b.getLong()).isEqualTo(0x0708090A0B0C0D0EL);
        assertThat(b.get()).isEqualTo((byte) 0x0F);
        assertThat(b.get()).isEqualTo((byte) 0x10);
        assertThat(b.get()).isEqualTo((byte) 0x11);
        assertThat(b.remaining()).isEqualTo(4);           // the CRC
    }

    /**
     * Property (SPEC §5.2): random frames, concatenated into one stream, then cut into random
     * chunks the way TCP might deliver them, decode to exactly the original frames, in order.
     */
    @Property(tries = 300)
    void survivesArbitraryFragmentation(@ForAll long seed) {
        Random random = new Random(seed);
        List<Frame> frames = new ArrayList<>();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int count = 1 + random.nextInt(20);
        for (int i = 0; i < count; i++) {
            byte[] payload = new byte[random.nextInt(4) == 0 ? 0 : random.nextInt(300)];
            random.nextBytes(payload);
            Frame f = new Frame((short) random.nextInt(), random.nextInt(Integer.MAX_VALUE), random.nextLong(),
                    (byte) random.nextInt(), Bytes.copyOf(payload));
            frames.add(f);
            stream.writeBytes(FrameCodec.encodeToArray(f));
        }
        byte[] all = stream.toByteArray();

        FrameDecoder decoder = new FrameDecoder();
        List<Frame> decoded = new ArrayList<>();
        int pos = 0;
        while (pos < all.length) {
            int chunk = random.nextInt(3) == 0 ? 1 : 1 + random.nextInt(64); // often single bytes
            chunk = Math.min(chunk, all.length - pos);
            decoded.addAll(decoder.feed(ByteBuffer.wrap(all, pos, chunk)));
            pos += chunk;
        }
        assertThat(decoded).as("seed %d", seed).isEqualTo(frames);
        assertThat(decoder.bufferedBytes()).isEqualTo(0);
    }

    /**
     * Flip every single bit of an encoded frame. The decoder must never return a wrong frame:
     * it either throws, or (only when the flip made the length field larger) keeps waiting for
     * bytes that never come. A flip anywhere outside the length field must always be detected.
     */
    @Test
    void detectsEverySingleBitFlip() {
        Frame frame = new Frame((short) 4, 2, 99L, (byte) 0, Bytes.ofUtf8("raft log entry"));
        byte[] wire = FrameCodec.encodeToArray(frame);
        for (int bit = 0; bit < wire.length * 8; bit++) {
            byte[] corrupt = wire.clone();
            corrupt[bit / 8] ^= (byte) (1 << (bit % 8));
            boolean inLengthField = bit / 8 < FrameCodec.LENGTH_FIELD_SIZE;
            List<Frame> result;
            try {
                result = new FrameDecoder().feed(corrupt);
            } catch (CodecException expected) {
                continue;
            }
            assertThat(inLengthField).as("undetected flip of bit %d", bit).isTrue();
            assertThat(result).as("bit %d produced a frame", bit).isEmpty();
        }
    }

    @Test
    void rejectsOversizedLengthBeforeTheBytesArrive() {
        FrameDecoder decoder = new FrameDecoder(1024);
        int tooBig = FrameCodec.MIN_LENGTH + 1025;
        byte[] header = ByteBuffer.allocate(4).putInt(tooBig).array();
        assertThatThrownBy(() -> decoder.feed(header))
                .isInstanceOf(CodecException.class).hasMessageContaining("invalid frame length");
    }

    @Test
    void acceptsPayloadOfExactlyTheMaximumSize() {
        Frame frame = new Frame((short) 1, 0, 0, (byte) 0, Bytes.copyOf(new byte[1024]));
        assertThat(new FrameDecoder(1024).feed(FrameCodec.encodeToArray(frame))).isEqualTo(List.of(frame));
    }

    @Test
    void rejectsLengthSmallerThanTheHeader() {
        byte[] header = ByteBuffer.allocate(4).putInt(FrameCodec.MIN_LENGTH - 1).array();
        assertThatThrownBy(() -> new FrameDecoder().feed(header)).isInstanceOf(CodecException.class);
    }

    @Test
    void refusesInputAfterAFailure() {
        FrameDecoder decoder = new FrameDecoder();
        byte[] bad = ByteBuffer.allocate(4).putInt(-1).array();
        assertThatThrownBy(() -> decoder.feed(bad)).isInstanceOf(CodecException.class);
        assertThatThrownBy(() -> decoder.feed(new byte[1])).isInstanceOf(IllegalStateException.class);
    }
}
