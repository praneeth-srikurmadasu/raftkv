package dev.raftkv.transport;

/** The transport itself failed (could not bind, selector broke). Individual lost messages are not errors. */
public final class TransportException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
