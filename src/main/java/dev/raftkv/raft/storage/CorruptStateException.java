package dev.raftkv.raft.storage;

/**
 * Durable state failed validation (bad CRC, impossible values, gaps). The node must not start:
 * running on state it can't trust could violate Raft's safety guarantees. An operator has to
 * repair or replace the node's data (e.g. wipe it and let it rejoin as an empty server).
 */
public final class CorruptStateException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CorruptStateException(String message) {
        super(message);
    }
}
