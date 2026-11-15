package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import dev.raftkv.client.ClientConfig;
import dev.raftkv.client.RaftKVClient;
import dev.raftkv.client.SessionExpiredException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.kv.KVResult;
import dev.raftkv.sim.Seeds;
import org.junit.jupiter.api.Test;

/** The KV store end to end: real client library, KV servers, Raft, simulated network. */
class KVClusterTest {

    private static final RaftConfig CONFIG = RaftConfig.defaults();
    private static final Duration WAIT = Duration.ofSeconds(30);

    private static Bytes b(String s) {
        return Bytes.ofUtf8(s);
    }

    private static SimulatedCluster cluster(long seed, int size) {
        SimulatedCluster c = SimulatedCluster.kv(seed, size, CONFIG, SimulatedCluster.Storage.MEMORY);
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(3));
        return c;
    }

    @Test
    void basicOperationsThroughTheClient() {
        long seed = Seeds.resolve();
        SimulatedCluster c = cluster(seed, 3);
        RaftKVClient client = c.newClient(ClientConfig.defaults());
        assertThat(c.await(client.putAsync(b("city"), b("Hyderabad")), WAIT)).isEqualTo(Optional.empty());
        assertThat(c.await(client.getAsync(b("city")), WAIT)).isEqualTo(Optional.of(b("Hyderabad")));
        assertThat(c.await(client.putAsync(b("city"), b("Pune")), WAIT)).isEqualTo(Optional.of(b("Hyderabad")));
        assertThat(c.await(client.casAsync(b("city"), Optional.of(b("Pune")), b("Delhi")), WAIT).success()).isTrue();
        assertThat(c.await(client.casAsync(b("city"), Optional.of(b("Pune")), b("Goa")), WAIT))
                .isEqualTo(new KVResult.CasResult(false, Optional.of(b("Delhi"))));
        assertThat(c.await(client.deleteAsync(b("city")), WAIT)).isTrue();
        assertThat(c.await(client.getAsync(b("city")), WAIT)).isEqualTo(Optional.empty());
    }

    @Test
    void clientThatOnlyKnowsAFollowerFindsTheLeader() {
        long seed = Seeds.resolve();
        SimulatedCluster c = cluster(seed, 5);
        RaftNode leader = c.convergedLeader(c.ids).orElseThrow();
        NodeId follower = c.ids.stream().filter(id -> !id.equals(leader.id())).findFirst().orElseThrow();
        RaftKVClient client = c.newClient(ClientConfig.defaults(), follower);
        c.await(client.putAsync(b("k"), b("v")), WAIT);
        assertThat(client.knownLeader()).as("seed %d", seed).isEqualTo(Optional.of(leader.id()));
    }

    @Test
    void operationsSurviveALeaderCrash() {
        long seed = Seeds.resolve();
        SimulatedCluster c = cluster(seed, 3);
        RaftKVClient client = c.newClient(ClientConfig.defaults());
        List<CompletableFuture<Optional<Bytes>>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(client.putAsync(b("k" + i), b("v" + i)));
            if (i == 25) {
                c.crash(c.leaders().get(0).id());
            }
            c.run(Duration.ofMillis(10));
        }
        for (CompletableFuture<Optional<Bytes>> f : futures) {
            c.await(f, WAIT);
        }
        for (int i = 0; i < 50; i++) {
            assertThat(c.await(client.getAsync(b("k" + i)), WAIT)).as("seed %d", seed).isEqualTo(Optional.of(b("v" + i)));
        }
    }

    /**
     * Exactly-once, observed from the outside. One client writes v1, v2, ... v40 to one key, one
     * at a time, over a network that drops and duplicates 20% of messages, with very short
     * client timeouts (so it retries constantly) and two leader crashes. Every PUT returns the
     * previous value: if any retry ever executed twice, some PUT would return its own value.
     */
    @Test
    void retriesNeverApplyAnOperationTwice() {
        long seed = Seeds.resolve();
        assertThat(exactlyOnceScenario(seed, CONFIG)).as("seed %d", seed).isEmpty();
    }

    static Optional<String> exactlyOnceScenario(long seed, RaftConfig config) {
        SimulatedCluster c = SimulatedCluster.kv(seed, 3, config, SimulatedCluster.Storage.MEMORY);
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(3));
        c.net.setDropRate(0.2);
        c.net.setDuplicateRate(0.2);
        ClientConfig impatient = new ClientConfig(Duration.ofMillis(40), Duration.ofMillis(5), Duration.ofMillis(100), Duration.ofSeconds(60));
        RaftKVClient client = c.newClient(impatient);
        Optional<Bytes> previous = Optional.empty();
        for (int i = 1; i <= 40; i++) {
            if (i == 15 || i == 30) {
                c.leaders().forEach(l -> c.crash(l.id()));
            }
            if (i == 20 || i == 35) {
                c.crashedNodes().forEach(c::restart);
            }
            Optional<Bytes> got = c.await(client.putAsync(b("k"), b("v" + i)), Duration.ofSeconds(60));
            if (!got.equals(previous)) {
                return Optional.of("PUT v" + i + " returned " + got.map(Bytes::toUtf8) + ", expected "
                        + previous.map(Bytes::toUtf8) + " (an operation was applied twice or lost)");
            }
            previous = Optional.of(b("v" + i));
        }
        if (client.retries() < 20) {
            return Optional.of("too few retries (" + client.retries() + ") to make the test meaningful");
        }
        // Every replica executed exactly 40 commands, no matter how many duplicates reached the log.
        c.crashedNodes().forEach(c::restart);
        c.net.setDropRate(0);
        c.net.setDuplicateRate(0);
        RaftNode leader = c.awaitLeader(Duration.ofSeconds(5));
        c.awaitAppliedEverywhere(leader.commitIndex(), Duration.ofSeconds(10));
        for (NodeId id : c.ids) {
            long executed = c.kvStore(id).executedCommands();
            if (executed != 40) {
                return Optional.of(id + " executed " + executed + " commands instead of 40");
            }
        }
        return Optional.empty();
    }

    @Test
    void manyConcurrentOperationsFromOneClientAllCompleteOnce() {
        long seed = Seeds.resolve();
        SimulatedCluster c = cluster(seed, 3);
        c.net.setDropRate(0.1);
        c.net.setDuplicateRate(0.1);
        RaftKVClient client = c.newClient(new ClientConfig(Duration.ofMillis(60), Duration.ofMillis(5), Duration.ofMillis(200), Duration.ofSeconds(60)));
        List<CompletableFuture<Optional<Bytes>>> futures = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            futures.add(client.putAsync(b("key-" + i), b("value-" + i)));
        }
        for (CompletableFuture<Optional<Bytes>> f : futures) {
            assertThat(c.await(f, Duration.ofSeconds(60))).as("seed %d: first write to a fresh key", seed).isEqualTo(Optional.empty());
        }
        c.net.setDropRate(0);
        c.net.setDuplicateRate(0);
        c.awaitLeader(Duration.ofSeconds(3));
        c.awaitIdenticalApplied(Duration.ofSeconds(10));
        for (NodeId id : c.ids) {
            assertThat(c.kvStore(id).size()).as("seed %d", seed).isEqualTo(200);
            assertThat(c.kvStore(id).executedCommands()).as("seed %d, %s", seed, id).isEqualTo(200L);
        }
        long d = c.kvStore(c.ids.get(0)).digest();
        for (NodeId id : c.ids) {
            assertThat(c.kvStore(id).digest()).as("replicas identical").isEqualTo(d);
        }
    }

    @Test
    void anOperationTimesOutWhenNoMajorityIsReachable() {
        long seed = Seeds.resolve();
        SimulatedCluster c = cluster(seed, 3);
        c.crash(c.ids.get(0));
        c.crash(c.ids.get(1));
        RaftKVClient client = c.newClient(new ClientConfig(Duration.ofMillis(100), Duration.ofMillis(10), Duration.ofMillis(200), Duration.ofSeconds(3)));
        CompletableFuture<Optional<Bytes>> f = client.putAsync(b("k"), b("v"));
        c.sim.runUntil(f::isDone, Duration.ofSeconds(10));
        assertThat(f.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(f::join).isInstanceOf(CompletionException.class).hasMessageContaining("TimeoutException");
        try {
            f.join();
        } catch (CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void anExpiredSessionIsReportedToTheApplication() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.kv(seed, 3, CONFIG, SimulatedCluster.Storage.MEMORY);
        c.setKvSessionTimeout(Duration.ofSeconds(2));
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(3));
        RaftKVClient sleepy = c.newClient(ClientConfig.defaults());
        RaftKVClient busy = c.newClient(ClientConfig.defaults());
        c.await(sleepy.putAsync(b("a"), b("1")), WAIT);
        for (int i = 0; i < 40; i++) {           // 4 s of log time pass while "sleepy" is idle
            c.await(busy.putAsync(b("b"), b("" + i)), WAIT);
            c.run(Duration.ofMillis(100));
        }
        CompletableFuture<Optional<Bytes>> late = sleepy.putAsync(b("a"), b("2"));
        c.sim.runUntil(late::isDone, WAIT);
        assertThatThrownBy(late::join).isInstanceOf(CompletionException.class);
        try {
            late.join();
        } catch (CompletionException e) {
            assertThat(e.getCause()).isInstanceOf(SessionExpiredException.class);
        }
        assertThat(c.await(busy.getAsync(b("a")), WAIT)).isEqualTo(Optional.of(b("1"))); // the late write did not happen
    }

    @Test
    void oversizedKeysAreRejectedBeforeLeavingTheClient() {
        SimulatedCluster c = cluster(1, 3);
        RaftKVClient client = c.newClient(ClientConfig.defaults());
        Bytes huge = Bytes.copyOf(new byte[2000]);
        assertThatThrownBy(() -> client.getAsync(huge)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void worksOnRealDiskStorageAcrossFullClusterRestarts() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.kv(seed, 3, CONFIG, SimulatedCluster.Storage.DISK);
        c.startAll();
        c.awaitLeader(Duration.ofSeconds(3));
        RaftKVClient client = c.newClient(ClientConfig.defaults());
        for (int i = 0; i < 30; i++) {
            c.await(client.putAsync(b("k" + i), b("v" + i)), WAIT);
        }
        c.ids.forEach(c::crash);
        c.ids.forEach(c::restart);
        for (int i = 0; i < 30; i++) {
            assertThat(c.await(client.getAsync(b("k" + i)), WAIT)).as("seed %d", seed).isEqualTo(Optional.of(b("v" + i)));
        }
    }

    /**
     * Regression (found by the sharded chaos test in Milestone 10): a client whose first request
     * timed out without ever reaching a server must still be able to use the cluster afterwards.
     * It used to send a watermark above 1 next time, which the servers read as "this session
     * expired" and refused forever.
     */
    @Test
    void aClientWhoseFirstRequestsNeverArrivedCanStillWorkLater() {
        long seed = Seeds.resolve();
        SimulatedCluster c = SimulatedCluster.kv(seed, 3, CONFIG, SimulatedCluster.Storage.MEMORY);
        c.startAllExcept(c.ids);
        RaftKVClient client = c.newClient(new ClientConfig(Duration.ofMillis(100), Duration.ofMillis(10),
                Duration.ofMillis(100), Duration.ofSeconds(1)));
        for (int i = 0; i < 3; i++) {
            CompletableFuture<Optional<Bytes>> lost = client.putAsync(b("a"), b("lost" + i));
            c.sim.runUntil(lost::isDone, WAIT);
            assertThatThrownBy(lost::join).isInstanceOf(CompletionException.class);
        }
        for (NodeId id : c.ids) {
            c.restart(id);
        }
        c.awaitLeader(Duration.ofSeconds(5));
        assertThat(c.await(client.putAsync(b("a"), b("1")), WAIT)).as("seed %d", seed).isEqualTo(Optional.empty());
        assertThat(c.await(client.getAsync(b("a")), WAIT)).isEqualTo(Optional.of(b("1")));
    }
}
