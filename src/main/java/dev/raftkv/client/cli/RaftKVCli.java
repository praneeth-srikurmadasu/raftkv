package dev.raftkv.client.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import dev.raftkv.Main;
import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.common.EventLoopScheduler;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.SystemClock;
import dev.raftkv.config.NodeConfig;
import dev.raftkv.transport.NioTransport;

/**
 * A command-line client.
 *
 * <pre>
 *   java -cp target/classes dev.raftkv.client.cli.RaftKVCli --servers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
 *   raftkv&gt; put color blue
 *   raftkv&gt; get color
 * </pre>
 *
 * Anything after the options is run as a single command, then the program exits (useful in
 * scripts): {@code ... RaftKVCli --servers ... put color blue}. Exit code 0 = success,
 * 1 = the command failed (e.g. no leader before the deadline), 2 = bad usage.
 */
public final class RaftKVCli implements AutoCloseable {

    static final String HELP = """
            commands:
              put <key> <value>              set a key (prints the previous value)
              get <key>                      read a key (linearizable)
              delete <key>                   remove a key
              cas <key> <expected|-> <new>   set only if the current value is <expected> ('-' = only if absent)
              leader                         the server this client believes is leader
              help                           this text
              quit                           exit""";

    static final String USAGE = """
            usage: raftkv-cli --servers <id=host:port,...> [--timeout-ms <n>] [command ...]
            """ + HELP;

    /** Clients use node ids far above any server id; the server replies to whatever id we announce. */
    private static final int CLIENT_ID_BASE = 1 << 24;

    private final RaftKVClient client;
    private final NioTransport transport;
    private final EventLoopScheduler loop;

    public RaftKVCli(Map<NodeId, InetSocketAddress> servers, Duration deadline) {
        NodeId me = new NodeId(CLIENT_ID_BASE + new SecureRandom().nextInt(Integer.MAX_VALUE - CLIENT_ID_BASE));
        loop = new EventLoopScheduler("raftkv-cli");
        RaftKVClient[] holder = new RaftKVClient[1];
        transport = NioTransport.client(me, servers, envelope -> holder[0].onMessage(envelope));
        ClientConfig defaults = ClientConfig.defaults();
        holder[0] = new RaftKVClient(transport, loop, SystemClock.INSTANCE, new SecureRandom(),
                new ArrayList<>(servers.keySet()), Main.DATA_GROUP,
                new ClientConfig(defaults.attemptTimeout(), defaults.backoffBase(), defaults.backoffMax(), deadline));
        client = holder[0];
    }

    public static void main(String[] args) throws IOException {
        String servers = null;
        long timeoutMs = 15_000;
        int i = 0;
        try {
            for (; i < args.length && args[i].startsWith("--"); i++) {
                switch (args[i]) {
                    case "--servers" -> servers = value(args, ++i);
                    case "--timeout-ms" -> timeoutMs = Long.parseLong(value(args, ++i));
                    case "--help" -> {
                        System.out.println(USAGE);
                        return;
                    }
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            if (servers == null) {
                throw new IllegalArgumentException("--servers is required");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(USAGE);
            System.exit(2);
            return;
        }
        List<String> command = Arrays.asList(args).subList(i, args.length);
        int exit;
        try (RaftKVCli cli = new RaftKVCli(NodeConfig.parsePeers(servers), Duration.ofMillis(timeoutMs))) {
            if (!command.isEmpty()) {
                exit = cli.run(command, System.out) ? 0 : 1;
            } else {
                cli.repl(new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)), System.out);
                exit = 0;
            }
        }
        System.exit(exit);
    }

    /** Reads commands until "quit" or end of input. */
    public void repl(BufferedReader in, PrintStream out) throws IOException {
        out.println("connected to group " + Main.DATA_GROUP + "; type 'help' for commands");
        while (true) {
            out.print("raftkv> ");
            out.flush();
            String line = in.readLine();
            if (line == null) {
                return;
            }
            List<String> words = Arrays.stream(line.trim().split("\\s+")).filter(w -> !w.isEmpty()).toList();
            if (words.isEmpty()) {
                continue;
            }
            if (words.get(0).equals("quit") || words.get(0).equals("exit")) {
                return;
            }
            run(words, out);
        }
    }

    /** Runs one command, printing the result; returns false if it failed. */
    public boolean run(List<String> words, PrintStream out) {
        String cmd = words.get(0).toLowerCase(java.util.Locale.ROOT);
        try {
            switch (cmd) {
                case "put" -> {
                    need(words, 3);
                    Optional<String> previous = client.put(words.get(1), String.join(" ", words.subList(2, words.size())));
                    out.println("OK" + previous.map(p -> " (was " + p + ")").orElse(""));
                }
                case "get" -> {
                    need(words, 2);
                    out.println(client.get(words.get(1)).orElse("(not found)"));
                }
                case "delete", "del" -> {
                    need(words, 2);
                    out.println(client.delete(words.get(1)) ? "deleted" : "(not found)");
                }
                case "cas" -> {
                    need(words, 4);
                    String expected = words.get(2).equals("-") ? null : words.get(2);
                    String newValue = String.join(" ", words.subList(3, words.size()));
                    out.println(client.cas(words.get(1), expected, newValue) ? "OK" : "FAILED (value did not match)");
                }
                case "leader" -> out.println(client.knownLeader().map(NodeId::toString).orElse("unknown (send a command first)"));
                case "help" -> out.println(HELP);
                default -> {
                    out.println("unknown command '" + cmd + "'; type 'help'");
                    return false;
                }
            }
            return true;
        } catch (IllegalArgumentException e) {
            out.println("error: " + e.getMessage());
            return false;
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            out.println("error: " + cause.getMessage());
            return false;
        }
    }

    public RaftKVClient client() {
        return client;
    }

    @Override
    public void close() {
        transport.close();
        loop.close();
    }

    private static void need(List<String> words, int n) {
        if (words.size() < n) {
            throw new IllegalArgumentException("'" + words.get(0) + "' needs " + (n - 1) + " argument(s); type 'help'");
        }
    }

    private static String value(String[] args, int i) {
        if (i >= args.length) {
            throw new IllegalArgumentException(args[i - 1] + " needs a value");
        }
        return args[i];
    }
}
