package dev.raftkv.kv;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.protocol.ClientStatus;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

class KVCodecTest {

    static Bytes randomBytes(Random r, int max) {
        byte[] b = new byte[r.nextInt(4) == 0 ? 0 : r.nextInt(max + 1)];
        r.nextBytes(b);
        return Bytes.copyOf(b);
    }

    static Optional<Bytes> randomOptional(Random r) {
        return r.nextBoolean() ? Optional.of(randomBytes(r, 50)) : Optional.empty();
    }

    static KVCommand randomCommand(Random r) {
        Bytes key = randomBytes(r, 40);
        return switch (r.nextInt(4)) {
            case 0 -> new KVCommand.Put(key, randomBytes(r, 100));
            case 1 -> new KVCommand.Get(key);
            case 2 -> new KVCommand.Delete(key);
            default -> new KVCommand.Cas(key, randomOptional(r), randomBytes(r, 100));
        };
    }

    static KVResult randomResult(Random r) {
        return switch (r.nextInt(4)) {
            case 0 -> new KVResult.PutResult(randomOptional(r));
            case 1 -> new KVResult.GetResult(randomOptional(r));
            case 2 -> new KVResult.DeleteResult(r.nextBoolean());
            default -> new KVResult.CasResult(r.nextBoolean(), randomOptional(r));
        };
    }

    @Property(tries = 1000)
    void commandsResultsAndPayloadsRoundTrip(@ForAll long seed) {
        Random r = new Random(seed);
        KVCommand command = randomCommand(r);
        assertThat(KVCodec.decodeCommand(KVCodec.encode(command))).as("seed %d", seed).isEqualTo(command);
        KVResult result = randomResult(r);
        assertThat(KVCodec.decodeResult(KVCodec.encode(result))).as("seed %d", seed).isEqualTo(result);
        SessionCommand sc = new SessionCommand(new UUID(r.nextLong(), r.nextLong()), r.nextLong(), r.nextLong(),
                r.nextLong(), KVCodec.encode(command));
        assertThat(KVCodec.decodeSessionCommand(KVCodec.encode(sc))).as("seed %d", seed).isEqualTo(sc);
        KVCodec.Outcome o = new KVCodec.Outcome(ClientStatus.OK, KVCodec.encode(result));
        assertThat(KVCodec.decodeOutcome(KVCodec.encode(o))).isEqualTo(o);
    }

    @Test
    void everyTruncationIsRejected() {
        Random r = new Random(3);
        for (int n = 0; n < 200; n++) {
            byte[] full = KVCodec.encode(randomCommand(r)).toArray();
            for (int cut = 0; cut < full.length; cut++) {
                Bytes prefix = Bytes.copyOf(Arrays.copyOf(full, cut));
                assertThatThrownBy(() -> KVCodec.decodeCommand(prefix)).isInstanceOf(CodecException.class);
            }
        }
    }

    @Test
    void sizeLimitsAreEnforcedOnConstructionAndOnDecode() {
        Bytes bigKey = Bytes.copyOf(new byte[KVCommand.MAX_KEY_BYTES + 1]);
        assertThatThrownBy(() -> new KVCommand.Get(bigKey)).isInstanceOf(IllegalArgumentException.class);
        Bytes okKey = Bytes.copyOf(new byte[KVCommand.MAX_KEY_BYTES]);
        assertThat(new KVCommand.Get(okKey).key().length()).isEqualTo(KVCommand.MAX_KEY_BYTES);
        Bytes bigValue = Bytes.copyOf(new byte[KVCommand.MAX_VALUE_BYTES + 1]);
        assertThatThrownBy(() -> new KVCommand.Put(okKey, bigValue)).isInstanceOf(IllegalArgumentException.class);
        // A hand-crafted payload with an oversized key decodes to a CodecException, not a crash.
        Bytes crafted = new dev.raftkv.codec.BinaryWriter().writeByte(2).writeBytes(bigKey).toBytes();
        assertThatThrownBy(() -> KVCodec.decodeCommand(crafted)).isInstanceOf(CodecException.class).hasMessageContaining("key");
    }

    @Test
    void unknownTagsAreRejected() {
        for (Bytes b : List.of(Bytes.copyOf(new byte[] {9}), Bytes.copyOf(new byte[] {0}))) {
            assertThatThrownBy(() -> KVCodec.decodeCommand(b)).isInstanceOf(CodecException.class);
            assertThatThrownBy(() -> KVCodec.decodeResult(b)).isInstanceOf(CodecException.class);
            assertThatThrownBy(() -> KVCodec.decodeSessionCommand(b)).isInstanceOf(CodecException.class);
        }
    }
}
