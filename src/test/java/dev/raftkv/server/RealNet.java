package dev.raftkv.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/** Helpers for tests that use real sockets, real files and real time. */
final class RealNet {

    private RealNet() {
    }

    /** A loopback address with a port that was free a moment ago. */
    static InetSocketAddress freeLoopbackAddress() {
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            s.setReuseAddress(true);
            return new InetSocketAddress(InetAddress.getLoopbackAddress(), s.getLocalPort());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Path tempDir(String prefix) {
        try {
            return Files.createTempDirectory(prefix);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Best effort: on Windows a just-killed process may still hold a file for a moment. */
    static void deleteQuietly(Path dir) {
        for (int attempt = 0; attempt < 5; attempt++) {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                return;
            } catch (IOException | UncheckedIOException e) {
                sleep(200);
            }
        }
    }

    /** Polls {@code condition} every 20 ms; fails with {@code what} after {@code maxMillis}. */
    static void await(String what, long maxMillis, BooleanSupplier condition) {
        long deadline = System.nanoTime() + maxMillis * 1_000_000;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out after " + maxMillis + " ms waiting for: " + what);
            }
            sleep(20);
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
