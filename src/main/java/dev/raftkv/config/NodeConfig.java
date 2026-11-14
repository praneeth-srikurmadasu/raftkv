package dev.raftkv.config;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import dev.raftkv.common.NodeId;

/**
 * How one server process is started (SPEC §5.3). Parsed from command-line arguments:
 *
 * <pre>
 *   --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 [--data data/n1] [--listen 0.0.0.0:7001] [--verbose]
 * </pre>
 *
 * <ul>
 *   <li>{@code --peers} lists every member of the cluster <em>including this node</em>.</li>
 *   <li>{@code --listen} defaults to this node's own address in {@code --peers}.</li>
 *   <li>{@code --data} defaults to {@code data/n<id>}.</li>
 * </ul>
 *
 * @param id      this node
 * @param peers   every member, including this node, in id order
 * @param listen  address to accept connections on
 * @param dataDir where the Raft log and metadata live
 * @param verbose also log the very frequent events (commits)
 */
public record NodeConfig(NodeId id, Map<NodeId, InetSocketAddress> peers, InetSocketAddress listen,
                         Path dataDir, boolean verbose) {

    public NodeConfig {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(listen, "listen");
        Objects.requireNonNull(dataDir, "dataDir");
        if (!peers.containsKey(id)) {
            throw new IllegalArgumentException("--peers must include this node (" + id.value() + ")");
        }
        peers = Collections.unmodifiableMap(new TreeMap<>(peers));
    }

    public static final String USAGE = """
            usage: raftkv --id <n> --peers <id=host:port,...> [--data <dir>] [--listen <host:port>] [--verbose]
              --id       this node's id (a number listed in --peers)
              --peers    every cluster member including this one, e.g. 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
              --data     data directory (default: data/n<id>)
              --listen   address to listen on (default: this node's address from --peers)
              --verbose  also log every commit""";

    /** Parses command-line arguments; throws {@link IllegalArgumentException} with a readable message. */
    public static NodeConfig parse(String... args) {
        Map<String, String> opts = new LinkedHashMap<>();
        boolean verbose = false;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--verbose" -> verbose = true;
                case "--id", "--peers", "--data", "--listen" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(a + " needs a value");
                    }
                    if (opts.put(a, args[++i]) != null) {
                        throw new IllegalArgumentException(a + " given twice");
                    }
                }
                default -> throw new IllegalArgumentException("unknown argument: " + a);
            }
        }
        if (!opts.containsKey("--id")) {
            throw new IllegalArgumentException("--id is required");
        }
        if (!opts.containsKey("--peers")) {
            throw new IllegalArgumentException("--peers is required");
        }
        NodeId id = new NodeId(parseInt(opts.get("--id"), "--id"));
        Map<NodeId, InetSocketAddress> peers = parsePeers(opts.get("--peers"));
        if (!peers.containsKey(id)) {
            throw new IllegalArgumentException("--peers must include this node (" + id.value() + ")");
        }
        InetSocketAddress listen = opts.containsKey("--listen") ? parseAddress(opts.get("--listen")) : peers.get(id);
        Path data = Path.of(opts.getOrDefault("--data", "data/n" + id.value()));
        return new NodeConfig(id, peers, listen, data, verbose);
    }

    /** Parses {@code 1=host:port,2=host:port}. */
    public static Map<NodeId, InetSocketAddress> parsePeers(String spec) {
        Map<NodeId, InetSocketAddress> peers = new TreeMap<>();
        for (String part : spec.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) {
                continue;
            }
            int eq = p.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("peer must look like id=host:port, got '" + p + "'");
            }
            NodeId id = new NodeId(parseInt(p.substring(0, eq), "peer id"));
            if (peers.put(id, parseAddress(p.substring(eq + 1))) != null) {
                throw new IllegalArgumentException("peer " + id.value() + " listed twice");
            }
        }
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("--peers is empty");
        }
        return peers;
    }

    /** Parses {@code host:port} (the host may be an IPv6 literal in brackets). */
    public static InetSocketAddress parseAddress(String s) {
        int colon = s.lastIndexOf(':');
        if (colon <= 0 || colon == s.length() - 1) {
            throw new IllegalArgumentException("address must look like host:port, got '" + s + "'");
        }
        String host = s.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        int port = parseInt(s.substring(colon + 1), "port");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        return new InetSocketAddress(host, port);
    }

    private static int parseInt(String s, String what) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(what + " must be a number, got '" + s + "'", e);
        }
    }
}
