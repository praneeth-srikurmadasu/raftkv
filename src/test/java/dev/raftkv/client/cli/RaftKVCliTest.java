package dev.raftkv.client.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.raftkv.server.LocalCluster;

class RaftKVCliTest {

    @Test
    void runsEveryCommandAgainstARealCluster() throws IOException {
        try (LocalCluster cluster = new LocalCluster(3, 2);
             RaftKVCli cli = new RaftKVCli(cluster.addresses(), Duration.ofSeconds(30))) {
            String script = String.join("\n",
                    "help",
                    "put color blue",
                    "put color light green",
                    "get color",
                    "cas color blue red",
                    "cas color light red",
                    "cas fresh - new",
                    "get fresh",
                    "delete color",
                    "delete color",
                    "get color",
                    "put onlykey",
                    "frobnicate",
                    "leader",
                    "config",
                    "shard fresh",
                    "admin move 0 2",
                    "admin bogus",
                    "quit",
                    "get fresh") + "\n";
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
            cli.repl(new BufferedReader(new StringReader(script)), out);
            String[] lines = bytes.toString(StandardCharsets.UTF_8).split("raftkv> ", -1);
            // lines[0] is the banner; then one chunk per command
            assertThat(lines[1]).contains("put <key> <value>");
            assertThat(lines[2].trim()).isEqualTo("OK");
            assertThat(lines[3].trim()).isEqualTo("OK (was blue)");
            assertThat(lines[4].trim()).isEqualTo("light green");
            assertThat(lines[5].trim()).isEqualTo("FAILED (value did not match)");
            assertThat(lines[6].trim()).isEqualTo("FAILED (value did not match)"); // expected is one word: "light"
            assertThat(lines[7].trim()).isEqualTo("OK");
            assertThat(lines[8].trim()).isEqualTo("new");
            assertThat(lines[9].trim()).isEqualTo("deleted");
            assertThat(lines[10].trim()).isEqualTo("(not found)");
            assertThat(lines[11].trim()).isEqualTo("(not found)");
            assertThat(lines[12]).contains("needs 2 argument(s)");
            assertThat(lines[13]).contains("unknown command 'frobnicate'");
            assertThat(lines[14]).contains("group 1:");
            assertThat(lines[14]).contains("group 2:");
            assertThat(lines[15]).contains("group 1 -> shards");
            assertThat(lines[15]).contains("on [n1, n2, n3]");
            assertThat(lines[16]).contains("-> group");
            assertThat(lines[17]).contains("now config");
            assertThat(lines[18]).contains("admin needs join, leave or move");
            assertThat(lines[19]).isEqualTo(""); // the prompt for "quit"; nothing ran after it
            assertThat(lines.length).isEqualTo(20);
            assertThat(cli.client().get("fresh").orElseThrow()).isEqualTo("new"); // shard 0 moved, data intact
        }
    }

    @Test
    void rangesAreCompact() {
        assertThat(RaftKVCli.ranges(java.util.List.of(0, 1, 2, 5, 7, 8))).isEqualTo("0-2,5,7-8");
        assertThat(RaftKVCli.ranges(java.util.List.of(3))).isEqualTo("3");
        assertThat(RaftKVCli.ranges(java.util.List.of())).isEqualTo("none");
    }

    @Test
    void oneShotCommandsReportSuccessAsABoolean() {
        try (LocalCluster cluster = new LocalCluster(3, 2);
             RaftKVCli cli = new RaftKVCli(cluster.addresses(), Duration.ofSeconds(30))) {
            PrintStream sink = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
            assertThat(cli.run(List.of("put", "a", "1"), sink)).isTrue();
            assertThat(cli.run(List.of("get"), sink)).isFalse();
            assertThat(cli.run(List.of("nope"), sink)).isFalse();
            assertThat(cli.run(List.of("admin", "join", "4", "n9"), sink)).as("unknown server id").isFalse();
            assertThat(cli.client().get("a").orElseThrow()).isEqualTo("1");
        }
    }
}
