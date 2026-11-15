package dev.raftkv.transport;

import java.util.UUID;

/**
 * Where a client that lives inside a server process registers to receive its replies: the
 * process's message router hands every {@code ClientReply} to the client whose id it echoes.
 */
@FunctionalInterface
public interface ClientRegistry {

    void register(UUID clientId, MessageHandler handler);
}
