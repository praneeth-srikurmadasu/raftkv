package dev.raftkv.kv;

import java.util.Objects;
import java.util.Optional;

import dev.raftkv.common.Bytes;

/** The result of a {@link KVCommand}, one type per command. */
public sealed interface KVResult {

    /** Result of {@link KVCommand.Put}: the value the key had before, if any. */
    record PutResult(Optional<Bytes> previous) implements KVResult {
        public PutResult {
            Objects.requireNonNull(previous, "previous");
        }
    }

    /** Result of {@link KVCommand.Get}. */
    record GetResult(Optional<Bytes> value) implements KVResult {
        public GetResult {
            Objects.requireNonNull(value, "value");
        }
    }

    /** Result of {@link KVCommand.Delete}. */
    record DeleteResult(boolean existed) implements KVResult {
    }

    /** Result of {@link KVCommand.Cas}: whether it was applied, and the key's value afterwards. */
    record CasResult(boolean success, Optional<Bytes> current) implements KVResult {
        public CasResult {
            Objects.requireNonNull(current, "current");
        }
    }
}
