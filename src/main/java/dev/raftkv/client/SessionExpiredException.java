package dev.raftkv.client;

/**
 * The cluster no longer remembers this client's session, so it cannot tell whether a retried
 * operation already took effect. The operation's outcome is unknown; create a new client
 * (a new session) and re-read the state before deciding what to do.
 */
public final class SessionExpiredException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SessionExpiredException(String message) {
        super(message);
    }
}
