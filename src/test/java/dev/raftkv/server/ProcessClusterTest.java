package dev.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import dev.raftkv.Main;
import dev.raftkv.client.cli.RaftKVCli;
import dev.raftkv.common.NodeId;

/**
 * Milestone 7 gate, part 2 (and the sharded cluster since Milestone 10): three real server
 * processes ({@code java dev.raftkv.Main}, each hosting the controller and three data groups), the
 * way the run-cluster scripts start them. The process leading group 1 is killed with no warning
 * ({@code destroyForcibly}, like {@code kill -9} or Task Manager's "End task"), twice, a shard is
 * moved, and every acknowledged write must still be readable.
 */
class ProcessClusterTest {

    private final Path root = RealNet.tempDir("raftkv-processes");
    private final Map<NodeId, InetSocketAddress> addresses = new TreeMap<>();
    private final Map<NodeId, Process> processes = new TreeMap<>();

    @Test
    void killingTheLeaderProcessLosesNoAcknowledgedWrite() throws Exception {
        for (int i = 1; i <= 3; i++) {
            addresses.put(NodeId.of(i), RealNet.freeLoopbackAddress());
        }
        try {
            for (NodeId id : addresses.keySet()) {
                launch(id);
            }
            try (RaftKVCli cli = new RaftKVCli(addresses, Duration.ofSeconds(30))) {
                List<String> written = new ArrayList<>();
                write(cli, "first", written);
                NodeId firstLeader = cli.client().knownLeader(1).orElseThrow();

                kill(firstLeader);
                write(cli, "second", written);
                NodeId secondLeader = cli.client().knownLeader(1).orElseThrow();
                assertThat(secondLeader).isNotEqualTo(firstLeader);

                launch(firstLeader);   // comes back from its data directory
                write(cli, "third", written);
                kill(secondLeader);    // now the restarted node is needed for a majority
                write(cli, "fourth", written);
                assertThat(cli.client().knownLeader(1).orElseThrow()).isNotEqualTo(secondLeader);

                // Move a shard while a node is down and the leader changed twice: data must follow.
                int moved = cli.client().controller().move(0, 2).join().gidOf(0);
                assertThat(moved).isEqualTo(2);

                for (String key : written) {
                    assertThat(cli.client().get(key)).as("key %s", key).isEqualTo(Optional.of("value-" + key));
                }
            }
        } catch (Throwable t) {
            System.out.println(logs());
            throw t;
        } finally {
            for (Process p : processes.values()) {
                p.destroyForcibly();
                p.waitFor(10, TimeUnit.SECONDS);
            }
            RealNet.deleteQuietly(root);
        }
    }

    private static void write(RaftKVCli cli, String prefix, List<String> written) {
        for (int i = 0; i < 15; i++) {
            String key = prefix + i;
            cli.client().put(key, "value-" + key);
            written.add(key);
        }
    }

    private void launch(NodeId id) throws IOException, URISyntaxException {
        String peers = addresses.entrySet().stream()
                .map(e -> e.getKey().value() + "=127.0.0.1:" + e.getValue().getPort())
                .collect(Collectors.joining(","));
        String classes = Path.of(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
        String java = Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java").toString();
        Path log = root.resolve("n" + id.value() + ".log");
        ProcessBuilder pb = new ProcessBuilder(java, "-Xmx256m", "-cp", classes, Main.class.getName(),
                "--id", String.valueOf(id.value()), "--peers", peers,
                "--data", root.resolve("n" + id.value()).toString())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()));
        processes.put(id, pb.start());
    }

    private void kill(NodeId id) throws InterruptedException {
        Process p = processes.remove(id);
        p.destroyForcibly();
        if (!p.waitFor(10, TimeUnit.SECONDS)) {
            throw new AssertionError("process of " + id + " did not die");
        }
    }

    private String logs() {
        StringBuilder sb = new StringBuilder();
        for (NodeId id : addresses.keySet()) {
            File f = root.resolve("n" + id.value() + ".log").toFile();
            sb.append("----- log of ").append(id).append(" -----\n");
            try {
                sb.append(Files.readString(f.toPath(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                sb.append("(unreadable: ").append(e.getMessage()).append(")\n");
            }
        }
        return sb.toString();
    }
}
