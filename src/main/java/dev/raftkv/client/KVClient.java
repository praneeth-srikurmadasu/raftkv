package dev.raftkv.client;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.common.Bytes;
import dev.raftkv.kv.KVResult;

/**
 * The key-value operations every RaftKV client offers: {@link RaftKVClient} (one group) and the
 * sharded client. Async methods can be called concurrently from any thread; the blocking
 * conveniences must not be called from the client's own event loop.
 */
public interface KVClient {

    /** Sets {@code key}; completes with the previous value, if any. */
    CompletableFuture<Optional<Bytes>> putAsync(Bytes key, Bytes value);

    /** Linearizable read of {@code key}. */
    CompletableFuture<Optional<Bytes>> getAsync(Bytes key);

    /** Removes {@code key}; completes with whether it existed. */
    CompletableFuture<Boolean> deleteAsync(Bytes key);

    /** Compare-and-set ({@code expected} empty = only if absent). */
    CompletableFuture<KVResult.CasResult> casAsync(Bytes key, Optional<Bytes> expected, Bytes newValue);

    default Optional<String> put(String key, String value) {
        return putAsync(Bytes.ofUtf8(key), Bytes.ofUtf8(value)).join().map(Bytes::toUtf8);
    }

    default Optional<String> get(String key) {
        return getAsync(Bytes.ofUtf8(key)).join().map(Bytes::toUtf8);
    }

    default boolean delete(String key) {
        return deleteAsync(Bytes.ofUtf8(key)).join();
    }

    default boolean cas(String key, String expected, String newValue) {
        return casAsync(Bytes.ofUtf8(key), Optional.ofNullable(expected).map(Bytes::ofUtf8), Bytes.ofUtf8(newValue))
                .join().success();
    }
}
