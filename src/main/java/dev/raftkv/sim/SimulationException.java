package dev.raftkv.sim;

/** Wraps a failure inside a simulated event, adding the virtual time and the seed to replay it. */
public final class SimulationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SimulationException(String message, Throwable cause) {
        super(message, cause);
    }
}
