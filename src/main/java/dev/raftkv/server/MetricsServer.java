package dev.raftkv.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.raftkv.common.GroupId;
import dev.raftkv.transport.NioTransport;

/**
 * Observability over HTTP (SPEC §8), with the JDK's built-in server (no dependencies):
 * <ul>
 *   <li>{@code /metrics}: plain text, one {@code name{labels} value} line per number, the format
 *       Prometheus scrapes;</li>
 *   <li>{@code /status.json}: the same numbers as JSON, for the dashboard;</li>
 *   <li>{@code /}: a live dashboard page that polls {@code /status.json} on every node of the
 *       cluster and draws groups × nodes (role, term, commit) and the shard map.</li>
 * </ul>
 * Every number is read on its group's event loop, so the Raft code stays single-threaded.
 */
public final class MetricsServer implements AutoCloseable {

    private final RaftHost host;
    private final NioTransport transport;
    private final HttpServer server;
    private final ExecutorService executor;

    /**
     * @param nodeUrls base URLs of every node's metrics server (this one included), for the dashboard
     */
    public static MetricsServer start(RaftHost host, NioTransport transport, InetSocketAddress address, List<String> nodeUrls)
            throws IOException {
        return new MetricsServer(host, transport, address, nodeUrls);
    }

    private MetricsServer(RaftHost host, NioTransport transport, InetSocketAddress address, List<String> nodeUrls)
            throws IOException {
        this.host = host;
        this.transport = transport;
        this.server = HttpServer.create(address, 16);
        this.executor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "raftkv-metrics");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);
        server.createContext("/metrics", e -> respond(e, "text/plain; version=0.0.4", prometheus()));
        server.createContext("/status.json", e -> respond(e, "application/json", json()));
        String dashboard = Dashboard.page(List.copyOf(nodeUrls));
        server.createContext("/", e -> respond(e, "text/html; charset=utf-8", dashboard));
        server.start();
    }

    public InetSocketAddress address() {
        return server.getAddress();
    }

    private List<Map<String, Object>> collect() {
        List<CompletableFuture<Map<String, Object>>> futures = new ArrayList<>();
        for (GroupId g : host.groups()) {
            futures.add(host.metrics(g));
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (CompletableFuture<Map<String, Object>> f : futures) {
            try {
                out.add(f.get(2, TimeUnit.SECONDS));
            } catch (Exception e) {
                // a group that is stopping or stuck is simply left out of this sample
            }
        }
        return out;
    }

    String prometheus() {
        StringBuilder sb = new StringBuilder();
        String node = String.valueOf(host.self().value());
        for (Map<String, Object> g : collect()) {
            String labels = "{node=\"" + node + "\",group=\"" + g.get("group") + "\"}";
            for (Map.Entry<String, Object> e : g.entrySet()) {
                Object v = e.getValue();
                if (e.getKey().equals("group")) {
                    continue;
                }
                if (v instanceof Number num) {
                    sb.append("raftkv_").append(e.getKey()).append(labels).append(' ').append(num).append('\n');
                } else if (v instanceof List<?> list) {
                    sb.append("raftkv_").append(e.getKey()).append("_count").append(labels).append(' ').append(list.size()).append('\n');
                } else if (e.getKey().equals("role")) {
                    sb.append("raftkv_is_leader").append(labels).append(' ').append("LEADER".equals(v) ? 1 : 0).append('\n');
                }
            }
        }
        String t = "{node=\"" + node + "\"}";
        sb.append("raftkv_frames_sent").append(t).append(' ').append(transport.framesSent()).append('\n');
        sb.append("raftkv_frames_received").append(t).append(' ').append(transport.framesReceived()).append('\n');
        sb.append("raftkv_frames_dropped").append(t).append(' ').append(transport.framesDropped()).append('\n');
        return sb.toString();
    }

    String json() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"node\":").append(host.self().value()).append(",\"groups\":[");
        boolean first = true;
        for (Map<String, Object> g : collect()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('{');
            boolean firstField = true;
            for (Map.Entry<String, Object> e : g.entrySet()) {
                if (!firstField) {
                    sb.append(',');
                }
                firstField = false;
                sb.append('"').append(e.getKey()).append("\":");
                Object v = e.getValue();
                if (v instanceof Number) {
                    sb.append(v);
                } else if (v instanceof List<?> list) {
                    sb.append('[');
                    for (int i = 0; i < list.size(); i++) {
                        sb.append(i > 0 ? "," : "").append(list.get(i));
                    }
                    sb.append(']');
                } else {
                    sb.append('"').append(String.valueOf(v).replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
                }
            }
            sb.append('}');
        }
        sb.append("],\"frames_sent\":").append(transport.framesSent())
                .append(",\"frames_received\":").append(transport.framesReceived())
                .append(",\"frames_dropped\":").append(transport.framesDropped()).append('}');
        return sb.toString();
    }

    private static void respond(HttpExchange e, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        e.getResponseHeaders().set("Content-Type", type);
        e.getResponseHeaders().set("Access-Control-Allow-Origin", "*"); // the dashboard polls every node
        e.getResponseHeaders().set("Cache-Control", "no-store");
        e.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = e.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
