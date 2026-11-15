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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.raftkv.client.AdminClient;
import dev.raftkv.client.ClientConfig;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.EventLoopScheduler;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.SystemClock;
import dev.raftkv.config.NodeConfig;
import dev.raftkv.shard.ControllerCommand;
import dev.raftkv.shard.ShardConfig;
import dev.raftkv.shard.ShardedKVClient;
import dev.raftkv.shard.Shards;
import dev.raftkv.transport.NioTransport;

/**
 * A command-line client for the sharded cluster.
 *
 * <pre>
 *   java -cp target/classes dev.raftkv.client.cli.RaftKVCli --servers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
 *   raftkv&gt; put color blue
 *   raftkv&gt; get color
 *   raftkv&gt; config
 *   raftkv&gt; admin move 5 2
 * </pre>
 *
 * {@code --servers} lists every server process (ids and addresses): the controller runs on them,
 * and configurations name data-group servers by id. Anything after the options is run as a single
 * command, then the program exits (useful in scripts). Exit code 0 = success, 1 = the command
 * failed (e.g. no answer before the deadline), 2 = bad usage.
 */
public final class RaftKVCli implements AutoCloseable {

    static final String HELP = """
            commands:
              put <key> <value>              set a key (prints the previous value)
              get <key>                      read a key (linearizable)
              delete <key>                   remove a key
              cas <key> <expected|-> <new>   set only if the current value is <expected> ('-' = only if absent)
              shard <key>                    which shard and group a key belongs to
              config                         the current configuration: which group serves which shards
              leader                         the leader this client knows for each group
              admin join <gid> <id,id,...>   give group <gid> (one the servers were started with, see --groups)
                                             back its share of shards; ids = the servers hosting it
              admin leave <gid>              remove a group; its shards move to the others
              admin add-server <gid> <id>    add server <id> to group <gid>'s Raft membership (learner, then voter)
              admin remove-server <gid> <id> remove server <id> from group <gid>'s Raft membership
              admin transfer-leader <gid> <id>  make server <id> the leader of group <gid>
              status                         every server's view of every group (role, term, commit, membership)
              admin move <shard> <gid>       move one shard to a group
              help                           this text
              quit                           exit""";

    static final String USAGE = """
            usage: raftkv-cli --servers <id=host:port,...> [--timeout-ms <n>] [command ...]
            """ + HELP;

    /** Clients use node ids far above any server id; the server replies to whatever id we announce. */
    private static final int CLIENT_ID_BASE = 1 << 24;

    private final ShardedKVClient client;
    private final AdminClient adminClient;
    private final Set<NodeId> servers;
    private final NioTransport transport;
    private final EventLoopScheduler loop;

    public RaftKVCli(Map<NodeId, InetSocketAddress> servers, Duration deadline) {
        this.servers = Set.copyOf(servers.keySet());
        NodeId me = new NodeId(CLIENT_ID_BASE + new SecureRandom().nextInt(Integer.MAX_VALUE - CLIENT_ID_BASE));
        loop = new EventLoopScheduler("raftkv-cli");
        ShardedKVClient[] holder = new ShardedKVClient[1];
        AdminClient[] admins = new AdminClient[1];
        transport = NioTransport.client(me, servers, envelope -> {
            holder[0].onMessage(envelope);  // each client ignores replies addressed to the other
            admins[0].onMessage(envelope);
        });
        admins[0] = new AdminClient(transport, loop, SystemClock.INSTANCE, new ArrayList<>(servers.keySet()));
        adminClient = admins[0];
        ClientConfig defaults = ClientConfig.defaults();
        holder[0] = new ShardedKVClient(transport, loop, SystemClock.INSTANCE, new SecureRandom(),
                new ArrayList<>(servers.keySet()),
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
        out.println("connected; type 'help' for commands");
        while (true) {
            out.print("raftkv> ");
            out.flush();
            String line = in.readLine();
            if (line == null) {
                return;
            }
            List<String> words = Arrays.stream(clean(line).split("\\s+")).filter(w -> !w.isEmpty()).toList();
            if (words.isEmpty()) {
                continue;
            }
            if (words.get(0).equals("quit") || words.get(0).equals("exit")) {
                return;
            }
            run(words, out);
        }
    }

    /**
     * Makes pasted examples work: drops a leading {@code raftkv>} prompt (even several) and a
     * trailing {@code # comment}.
     */
    static String clean(String line) {
        String s = line;
        int hash = s.indexOf('#');
        if (hash >= 0) {
            s = s.substring(0, hash);
        }
        s = s.trim();
        while (s.startsWith("raftkv>")) {
            s = s.substring("raftkv>".length()).trim();
        }
        return s;
    }

    /** Runs one command, printing the result; returns false if it failed. */
    public boolean run(List<String> words, PrintStream out) {
        String cmd = words.get(0).toLowerCase(Locale.ROOT);
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
                case "shard" -> {
                    need(words, 2);
                    ShardConfig c = latestConfig();
                    int shard = Shards.shardOf(Bytes.ofUtf8(words.get(1)), c.numShards());
                    out.println("shard " + shard + " -> group " + c.gidOf(shard) + " (config " + c.num() + ")");
                }
                case "config" -> out.println(describe(latestConfig()));
                case "leader" -> {
                    ShardConfig c = latestConfig();
                    StringBuilder sb = new StringBuilder();
                    for (int gid : c.gids()) {
                        sb.append("group ").append(gid).append(": ")
                                .append(client.knownLeader(gid).map(NodeId::toString).orElse("unknown (no request sent yet)"))
                                .append('\n');
                    }
                    out.print(sb.length() == 0 ? "no groups yet\n" : sb.toString());
                }
                case "admin" -> {
                    return admin(words, out);
                }
                case "status" -> status(out);
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

    /** Asks every server for its replica's status in every group (group 0 is the controller). */
    private void status(PrintStream out) {
        List<Integer> gids = new ArrayList<>();
        gids.add(0);
        try {
            gids.addAll(latestConfig().gids());
        } catch (CompletionException e) {
            out.println("(controller unreachable: showing group 0 only)");
        }
        for (int gid : gids) {
            List<CompletableFuture<String>> lines = new ArrayList<>();
            for (NodeId server : new TreeSet<>(servers)) {
                lines.add(adminClient.status(GroupId.of(gid), server, Duration.ofSeconds(1))
                        .exceptionally(e -> "g" + gid + " " + server + " unreachable"));
            }
            for (CompletableFuture<String> line : lines) {
                out.println(line.join());
            }
        }
    }

    private boolean admin(List<String> words, PrintStream out) {
        need(words, 2);
        String sub = words.get(1).toLowerCase(Locale.ROOT);
        if (sub.equals("add-server") || sub.equals("remove-server") || sub.equals("transfer-leader")) {
            need(words, 4);
            GroupId g = GroupId.of(parseInt(words.get(2), "gid"));
            NodeId id = new NodeId(parseInt(words.get(3).trim().replaceFirst("^n", ""), "server id"));
            if (!servers.contains(id)) {
                throw new IllegalArgumentException("server " + id + " is not one of --servers " + new TreeSet<>(servers));
            }
            Duration d = Duration.ofSeconds(30);
            CompletableFuture<String> r = switch (sub) {
                case "add-server" -> adminClient.addServer(g, id, d);
                case "remove-server" -> adminClient.removeServer(g, id, d);
                default -> adminClient.transferLeadership(g, id, d);
            };
            out.println(r.join());
            return true;
        }
        CompletableFuture<ShardConfig> result = switch (sub) {
            case "join" -> {
                need(words, 4);
                List<NodeId> ids = new ArrayList<>();
                for (String id : words.get(3).split(",")) {
                    NodeId server = new NodeId(parseInt(id.trim().replaceFirst("^n", ""), "server id"));
                    if (!servers.contains(server)) {
                        // a configuration naming a server that doesn't exist would leave its shards unreachable
                        throw new IllegalArgumentException("server " + server + " is not one of --servers " + new TreeSet<>(servers));
                    }
                    ids.add(server);
                }
                TreeMap<Integer, List<NodeId>> groups = new TreeMap<>();
                groups.put(parseInt(words.get(2), "gid"), ids);
                yield client.controller().join(groups);
            }
            case "leave" -> {
                need(words, 3);
                yield client.controller().leave(List.of(parseInt(words.get(2), "gid")));
            }
            case "move" -> {
                need(words, 4);
                yield client.controller().move(parseInt(words.get(2), "shard"), parseInt(words.get(3), "gid"));
            }
            default -> throw new IllegalArgumentException("admin needs join, leave, move, add-server, remove-server or transfer-leader; type 'help'");
        };
        out.println("now " + describe(result.join()));
        return true;
    }

    private ShardConfig latestConfig() {
        return client.controller().query(ControllerCommand.Query.LATEST).join();
    }

    /** {@code config 4: group 1 -> shards 0-20 on [n1, n2, n3]; group 2 -> ...}. */
    static String describe(ShardConfig c) {
        if (c.groups().isEmpty()) {
            return "config " + c.num() + ": no groups yet (shards unassigned)";
        }
        StringBuilder sb = new StringBuilder("config " + c.num() + ":");
        for (int gid : c.gids()) {
            sb.append("\n  group ").append(gid).append(" -> shards ").append(ranges(c.shardsOf(gid)))
                    .append(" on ").append(c.servers(gid));
        }
        List<Integer> unassigned = c.shardsOf(0);
        if (!unassigned.isEmpty()) {
            sb.append("\n  unassigned -> shards ").append(ranges(unassigned));
        }
        return sb.toString();
    }

    /** [0,1,2,5,7,8] -> "0-2,5,7-8". */
    static String ranges(List<Integer> shards) {
        if (shards.isEmpty()) {
            return "none";
        }
        StringBuilder sb = new StringBuilder();
        int start = shards.get(0);
        int prev = start;
        for (int i = 1; i <= shards.size(); i++) {
            int cur = i < shards.size() ? shards.get(i) : Integer.MIN_VALUE;
            if (cur != prev + 1) {
                sb.append(sb.length() == 0 ? "" : ",").append(start == prev ? String.valueOf(start) : start + "-" + prev);
                start = cur;
            }
            prev = cur;
        }
        return sb.toString();
    }

    public ShardedKVClient client() {
        return client;
    }

    @Override
    public void close() {
        transport.close();
        loop.close();
    }

    private static void need(List<String> words, int n) {
        if (words.size() < n) {
            throw new IllegalArgumentException("'" + String.join(" ", words.subList(0, Math.min(2, words.size())))
                    + "' needs " + (n - 1) + " argument(s); type 'help'");
        }
    }

    private static int parseInt(String s, String what) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(what + " must be a number, got '" + s + "'", e);
        }
    }

    private static String value(String[] args, int i) {
        if (i >= args.length) {
            throw new IllegalArgumentException(args[i - 1] + " needs a value");
        }
        return args[i];
    }
}
