package dev.raftkv.kv;

import java.util.Optional;
import java.util.UUID;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.protocol.ClientStatus;

/**
 * Binary encodings of the KV layer: commands, results, log-entry payloads, and apply outcomes.
 *
 * <p>Same rules as the network codec: explicit, versioned by a leading tag byte, and every
 * decoder rejects truncated input and trailing bytes with {@link CodecException}.
 */
public final class KVCodec {

    private static final byte PUT = 1;
    private static final byte GET = 2;
    private static final byte DELETE = 3;
    private static final byte CAS = 4;

    private static final byte SESSION_COMMAND = 1;

    private KVCodec() {
    }

    // ------------------------------------------------------------------ commands

    public static Bytes encode(KVCommand command) {
        BinaryWriter w = new BinaryWriter();
        switch (command) {
            case KVCommand.Put p -> {
                w.writeByte(PUT);
                w.writeBytes(p.key());
                w.writeBytes(p.value());
            }
            case KVCommand.Get g -> {
                w.writeByte(GET);
                w.writeBytes(g.key());
            }
            case KVCommand.Delete d -> {
                w.writeByte(DELETE);
                w.writeBytes(d.key());
            }
            case KVCommand.Cas c -> {
                w.writeByte(CAS);
                w.writeBytes(c.key());
                writeOptional(w, c.expected());
                w.writeBytes(c.newValue());
            }
        }
        return w.toBytes();
    }

    public static KVCommand decodeCommand(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        KVCommand command;
        try {
            byte tag = r.readByte();
            command = switch (tag) {
                case PUT -> new KVCommand.Put(r.readBytes(), r.readBytes());
                case GET -> new KVCommand.Get(r.readBytes());
                case DELETE -> new KVCommand.Delete(r.readBytes());
                case CAS -> new KVCommand.Cas(r.readBytes(), readOptional(r), r.readBytes());
                default -> throw new CodecException("unknown KV command tag " + tag);
            };
        } catch (IllegalArgumentException e) {
            throw new CodecException("invalid KV command: " + e.getMessage());
        }
        r.expectEnd();
        return command;
    }

    // ------------------------------------------------------------------ results

    public static Bytes encode(KVResult result) {
        BinaryWriter w = new BinaryWriter();
        switch (result) {
            case KVResult.PutResult p -> {
                w.writeByte(PUT);
                writeOptional(w, p.previous());
            }
            case KVResult.GetResult g -> {
                w.writeByte(GET);
                writeOptional(w, g.value());
            }
            case KVResult.DeleteResult d -> {
                w.writeByte(DELETE);
                w.writeBoolean(d.existed());
            }
            case KVResult.CasResult c -> {
                w.writeByte(CAS);
                w.writeBoolean(c.success());
                writeOptional(w, c.current());
            }
        }
        return w.toBytes();
    }

    public static KVResult decodeResult(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        byte tag = r.readByte();
        KVResult result = switch (tag) {
            case PUT -> new KVResult.PutResult(readOptional(r));
            case GET -> new KVResult.GetResult(readOptional(r));
            case DELETE -> new KVResult.DeleteResult(r.readBoolean());
            case CAS -> new KVResult.CasResult(r.readBoolean(), readOptional(r));
            default -> throw new CodecException("unknown KV result tag " + tag);
        };
        r.expectEnd();
        return result;
    }

    // ------------------------------------------------------------------ log payloads

    public static Bytes encode(SessionCommand c) {
        return new BinaryWriter()
                .writeByte(SESSION_COMMAND)
                .writeLong(c.clientId().getMostSignificantBits())
                .writeLong(c.clientId().getLeastSignificantBits())
                .writeLong(c.sequence())
                .writeLong(c.ackedBelow())
                .writeLong(c.timestampMillis())
                .writeBytes(c.command())
                .toBytes();
    }

    public static SessionCommand decodeSessionCommand(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        byte tag = r.readByte();
        if (tag != SESSION_COMMAND) {
            throw new CodecException("unknown log payload tag " + tag);
        }
        SessionCommand c = new SessionCommand(new UUID(r.readLong(), r.readLong()), r.readLong(), r.readLong(),
                r.readLong(), r.readBytes());
        r.expectEnd();
        return c;
    }

    // ------------------------------------------------------------------ apply outcomes

    /** What the state machine returns to Raft for one entry: a status and (if OK) the result. */
    public record Outcome(ClientStatus status, Bytes result) {
    }

    public static Bytes encode(Outcome o) {
        return new BinaryWriter().writeByte(o.status().code()).writeBytes(o.result()).toBytes();
    }

    public static Outcome decodeOutcome(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        Outcome o = new Outcome(ClientStatus.fromCode(r.readByte()), r.readBytes());
        r.expectEnd();
        return o;
    }

    // ------------------------------------------------------------------ helpers

    private static void writeOptional(BinaryWriter w, Optional<Bytes> value) {
        w.writeBoolean(value.isPresent());
        value.ifPresent(w::writeBytes);
    }

    private static Optional<Bytes> readOptional(BinaryReader r) {
        return r.readBoolean() ? Optional.of(r.readBytes()) : Optional.empty();
    }
}
