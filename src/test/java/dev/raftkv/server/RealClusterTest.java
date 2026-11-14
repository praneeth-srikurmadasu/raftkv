package dev.raftkv.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import dev.raftkv.Main;
import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.common.EventLoopScheduler;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.SystemClock;
import dev.raftkv.raft.Role;
import dev.raftkv.transport.NioTransport;

/**
 * Milestone 7 gate, part 1: the same Raft and KV code as in the simulator, now on real TCP,
 * real files and real time, three nodes in one JVM.
 */
class RealClusterTest {

    /** A client wired the way the CLI wires it. */
    private static final class Client implements AutoCloseable {
        final EventLoopScheduler loop = new EventLoopScheduler("test-client");
        final NioTransport transport;
        final RaftKVClient kv;

        Client(LocalCluster cluster) {
            RaftKVClient[] holder = new RaftKVClient[1];
            transport = NioTransport.client(NodeId.of(9_000_000), cluster.addresses(), e -> holder[0].onMessage(e));
            ClientConfig d = ClientConfig.defaults();
            holder[0] = new RaftKVClient(transport, loop, SystemClock.INSTANCE, new SecureRandom(),
                    new ArrayList<>(cluster.ids()), Main.DATA_GROUP,
                    new ClientConfig(d.attemptTimeout(), d.backoffBase(), d.backoffMax(), Duration.ofSeconds(30)));
            kv = holder[0];
        }

        @Override
        public void close() {
            transport.close();
            loop.close();
        }
    }

    @Test
    void electsOneLeaderAndServesReadsAndWrites() {
        try (LocalCluster cluster = new LocalCluster(3); Client client = new Client(cluster)) {
            NodeId leader = cluster.awaitLeader();
            assertThat(cluster.status(leader).role()).isEqualTo(Role.LEADER);

            for (int i = 0; i < 100; i++) {
                assertThat(client.kv.put("k" + i, "v" + i)).isEqualTo(Optional.empty());
            }
            assertThat(client.kv.put("k0", "changed")).isEqualTo(Optional.of("v0"));
            assertThat(client.kv.cas("k1", "v1", "swapped")).isTrue();
            assertThat(client.kv.cas("k1", "v1", "again")).isFalse();
            assertThat(client.kv.delete("k2")).isTrue();
            assertThat(client.kv.get("k0")).isEqualTo(Optional.of("changed"));
            assertThat(client.kv.get("k1")).isEqualTo(Optional.of("swapped"));
            assertThat(client.kv.get("k2")).isEqualTo(Optional.empty());
            assertThat(client.kv.get("k99")).isEqualTo(Optional.of("v99"));

            cluster.awaitConverged();
            for (NodeId id : cluster.ids()) {
                assertThat(cluster.status(id).keys()).as("keys on %s", id).isEqualTo(99);
            }
            assertThat(cluster.fatalErrors().isEmpty()).isTrue();
        }
    }

    @Test
    void survivesTheLeaderStoppingAndCatchesItUpWhenItReturns() {
        try (LocalCluster cluster = new LocalCluster(3); Client client = new Client(cluster)) {
            for (int i = 0; i < 20; i++) {
                client.kv.put("before" + i, "x" + i);
            }
            NodeId oldLeader = cluster.awaitLeader();
            cluster.stop(oldLeader);

            for (int i = 0; i < 20; i++) {
                client.kv.put("during" + i, "y" + i);
            }
            NodeId newLeader = cluster.awaitLeader();
            assertThat(newLeader).isNotEqualTo(oldLeader);

            cluster.start(oldLeader); // same port, same data directory: recovers its log from disk
            for (int i = 0; i < 20; i++) {
                client.kv.put("after" + i, "z" + i);
            }
            cluster.awaitConverged();
            for (NodeId id : cluster.ids()) {
                assertThat(cluster.status(id).keys()).as("keys on %s", id).isEqualTo(60);
            }
            for (int i = 0; i < 20; i++) {
                assertThat(client.kv.get("before" + i)).isEqualTo(Optional.of("x" + i));
                assertThat(client.kv.get("during" + i)).isEqualTo(Optional.of("y" + i));
                assertThat(client.kv.get("after" + i)).isEqualTo(Optional.of("z" + i));
            }
            assertThat(cluster.fatalErrors().isEmpty()).isTrue();
        }
    }
}
