package dev.raftkv.server;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import dev.raftkv.Main;
import dev.raftkv.common.NodeId;

/**
 * Real server processes ({@code java dev.raftkv.Main}) on loopback ports, the way the scripts run
 * them, for tests and Jepsen-lite. {@link #kill} is {@code destroyForcibly}: kill -9, no shutdown.
 */
public final class ProcessCluster implements AutoCloseable {

    private final Path root = RealNet.tempDir("raftkv-processes");
    private final Map<NodeId, InetSocketAddress> addresses = new TreeMap<>();
    private final Map<NodeId, Process> processes = new TreeMap<>();

    public ProcessCluster(int size) {
        for (int i = 1; i <= size; i++) {
            addresses.put(NodeId.of(i), RealNet.freeLoopbackAddress());
        }
    }

    public Map<NodeId, InetSocketAddress> addresses() {
        return addresses;
    }

    public List<NodeId> ids() {
        return List.copyOf(addresses.keySet());
    }

    public boolean isRunning(NodeId id) {
        return processes.containsKey(id);
    }

    public void startAll() {
        addresses.keySet().forEach(this::start);
    }

    public void start(NodeId id) {
        if (processes.containsKey(id)) {
            return;
        }
        String peers = addresses.entrySet().stream()
                .map(e -> e.getKey().value() + "=127.0.0.1:" + e.getValue().getPort())
                .collect(Collectors.joining(","));
        try {
            String classes = Path.of(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
            boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
            String java = Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java").toString();
            Path log = root.resolve("n" + id.value() + ".log");
            ProcessBuilder pb = new ProcessBuilder(java, "-Xmx256m", "-cp", classes, Main.class.getName(),
                    "--id", String.valueOf(id.value()), "--peers", peers, "--metrics-port", "0",
                    "--data", root.resolve("n" + id.value()).toString())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()));
            processes.put(id, pb.start());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    /** kill -9: the process gets no chance to clean up. */
    public void kill(NodeId id) {
        Process p = processes.remove(id);
        if (p == null) {
            return;
        }
        p.destroyForcibly();
        try {
            if (!p.waitFor(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("process of " + id + " did not die");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Every node's log, for failure reports. */
    public String logs() {
        StringBuilder sb = new StringBuilder();
        for (NodeId id : addresses.keySet()) {
            sb.append("----- log of ").append(id).append(" -----\n");
            try {
                sb.append(Files.readString(root.resolve("n" + id.value() + ".log"), StandardCharsets.UTF_8));
            } catch (IOException e) {
                sb.append("(unreadable: ").append(e.getMessage()).append(")\n");
            }
        }
        return sb.toString();
    }

    @Override
    public void close() {
        for (NodeId id : List.copyOf(processes.keySet())) {
            kill(id);
        }
        RealNet.deleteQuietly(root);
    }
}
