package dev.raftkv.protocol;

import java.util.Objects;
import java.util.UUID;

import dev.raftkv.common.Bytes;

/**
 * A client operation sent to a server (SPEC §4.2, exactly-once semantics).
 *
 * @param clientId   random 128-bit id of the client session
 * @param sequence   per-client request number, starting at 1; a retry reuses the same number
 * @param ackedBelow the lowest sequence number the client has NOT yet received a reply for;
 *                   the server may forget cached results below it (thesis §6.3)
 * @param command    the encoded operation (a {@code KVCommand} for the KV store)
 */
public record ClientRequest(UUID clientId, long sequence, long ackedBelow, Bytes command) implements Message {

    public ClientRequest {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(command, "command");
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence starts at 1, was " + sequence);
        }
        if (ackedBelow < 1 || ackedBelow > sequence) {
            throw new IllegalArgumentException("need 1 <= ackedBelow <= sequence, got " + ackedBelow + " and " + sequence);
        }
    }
}
