package dev.raftkv.io;

/**
 * An I/O failure. Unchecked: a storage error in Raft is not something to handle locally (the
 * node can't safely continue without its durable state), so it propagates and stops the node.
 */
public class DiskException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DiskException(String message) {
        super(message);
    }

    public DiskException(String message, Throwable cause) {
        super(message, cause);
    }
}
