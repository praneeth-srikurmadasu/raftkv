package dev.raftkv.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.common.Clock;
import dev.raftkv.common.EventLoopScheduler;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Tracer;
import dev.raftkv.shard.ControllerClient;
import dev.raftkv.shard.ControllerCommand;
import dev.raftkv.transport.ClientRegistry;
import dev.raftkv.transport.Transport;

/**
 * Gives a brand-new cluster its first configuration (SPEC §6.3): the controller starts with
 * configuration 0, which has no groups. The node with the lowest id asks the controller, once a
 * second, until a configuration with groups exists; if there is none yet, it sends one Join with
 * every data group on every peer. A Join through the controller's session is exactly-once, and
 * only one node ever tries, so the cluster gets exactly one initial configuration. After that,
 * groups are changed only by explicit admin commands ({@code admin join/leave/move} in the CLI).
 */
public final class ClusterBootstrap implements AutoCloseable {

    private final EventLoopScheduler loop = new EventLoopScheduler("raftkv-bootstrap");
    private final ControllerClient controller;
    private final SortedMap<Integer, List<NodeId>> groups;
    private final Tracer tracer;
    private volatile boolean done;

    public ClusterBootstrap(NodeId self, List<NodeId> peers, int groupCount, Transport transport, ClientRegistry registry,
                            Clock clock, Tracer tracer) {
        Objects.requireNonNull(self, "self");
        this.tracer = Objects.requireNonNull(tracer, "tracer");
        TreeMap<Integer, List<NodeId>> g = new TreeMap<>();
        for (int gid = 1; gid <= groupCount; gid++) {
            g.put(gid, new ArrayList<>(peers));
        }
        this.groups = g;
        ClientConfig cc = new ClientConfig(Duration.ofMillis(500), Duration.ofMillis(50), Duration.ofSeconds(1),
                Duration.ofSeconds(5));
        this.controller = new ControllerClient(transport, loop, clock, new Random(), peers, cc);
        registry.register(controller.clientId(), controller::onMessage);
        if (!self.equals(peers.stream().min(NodeId::compareTo).orElseThrow())) {
            done = true; // only the lowest id bootstraps
        }
    }

    /** Starts trying (in the background) if this node is the one that bootstraps. */
    public void start() {
        if (!done) {
            loop.schedule(Duration.ofSeconds(1), this::attempt);
        }
    }

    private void attempt() {
        if (done) {
            return;
        }
        controller.query(ControllerCommand.Query.LATEST).whenComplete((latest, error) -> {
            if (latest != null && !latest.groups().isEmpty()) {
                done = true;
                return;
            }
            if (latest == null) {
                loop.schedule(Duration.ofSeconds(1), this::attempt);
                return;
            }
            controller.join(groups).whenComplete((config, e) -> {
                if (config != null) {
                    done = true;
                    tracer.trace("bootstrap: created " + config);
                } else {
                    loop.schedule(Duration.ofSeconds(1), this::attempt);
                }
            });
        });
    }

    @Override
    public void close() {
        done = true;
        loop.close();
    }
}
