package dev.raftkv.kv;

import java.util.Objects;
import java.util.UUID;

import dev.raftkv.common.Bytes;

/**
 * What the KV server puts into the Raft log for one client request: the client's identity and
 * sequence numbers (for exactly-once), the leader's timestamp (for session expiry), and the
 * encoded {@link KVCommand}.
 *
 * @param clientId        the client session
 * @param sequence        the request's sequence number
 * @param ackedBelow      the client's acknowledgement watermark (see {@code ClientRequest})
 * @param timestampMillis the leader's wall clock when it accepted the request
 * @param command         the encoded command (decoded by the state machine; may be invalid)
 */
public record SessionCommand(UUID clientId, long sequence, long ackedBelow, long timestampMillis, Bytes command) {

    public SessionCommand {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(command, "command");
    }
}
