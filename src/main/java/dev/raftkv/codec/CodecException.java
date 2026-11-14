package dev.raftkv.codec;

/**
 * Thrown when bytes from the network or disk cannot be decoded: truncated data, bad CRC,
 * an unknown message type, an impossible length, invalid UTF-8, trailing garbage...
 *
 * <p>Decoding code never lets a low-level {@code BufferUnderflowException} or
 * {@code IndexOutOfBoundsException} escape: input we did not create is untrusted, and a
 * single, explicit exception type makes "close the connection / reject the record" easy.
 */
public final class CodecException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CodecException(String message) {
        super(message);
    }
}
