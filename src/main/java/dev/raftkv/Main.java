package dev.raftkv;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.SystemClock;
import dev.raftkv.common.Tracer;
import dev.raftkv.config.NodeConfig;
import dev.raftkv.io.RealDisk;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.server.RaftHost;
import dev.raftkv.transport.NioTransport;

/**
 * Starts one RaftKV server process (Milestone 7: one Raft group, real TCP, real disk).
 *
 * <pre>
 *   java -cp target/classes dev.raftkv.Main --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
 * </pre>
 *
 * With no arguments (or {@code --version}) it prints the version banner, as in Milestone 0.
 * Logs one line per important Raft event to standard output. Ctrl+C stops the node cleanly;
 * killing the process is also safe, because everything acknowledged is already fsynced.
 */
public final class Main {

    /** Project version; kept in sync with pom.xml by hand until we need more. */
    public static final String VERSION = "0.1.0-SNAPSHOT";

    /** Lowest Java feature release we support (records, sealed types, virtual threads, pattern switch). */
    public static final int MIN_JAVA_FEATURE = 21;

    /** The single data group until sharding arrives (group 0 is reserved for the controller). */
    public static final GroupId DATA_GROUP = GroupId.of(1);

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Main() {
    }

    public static void main(String[] args) throws InterruptedException {
        int feature = Runtime.version().feature();
        if (feature < MIN_JAVA_FEATURE) {
            System.err.println("RaftKV needs Java " + MIN_JAVA_FEATURE + "+, but this is Java " + feature);
            System.exit(1);
        }
        if (args.length == 0 || args[0].equals("--version") || args[0].equals("--help")) {
            System.out.println(banner());
            if (args.length == 0 || args[0].equals("--help")) {
                System.out.println(NodeConfig.USAGE);
            }
            return;
        }
        NodeConfig config;
        try {
            config = NodeConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(NodeConfig.USAGE);
            System.exit(2);
            return;
        }
        boolean verbose = config.verbose();
        Tracer log = event -> {
            if (verbose || !isNoisy(event)) {
                System.out.println(LocalTime.now().format(TIME) + " " + event);
            }
        };

        CountDownLatch shutdownRequested = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Ctrl+C or a normal kill: let main() close everything, then let the JVM exit.
            shutdownRequested.countDown();
            try {
                closed.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "raftkv-shutdown"));

        try (RaftHost host = new RaftHost(config.id(), new RealDisk(), config.dataDir(), SystemClock.INSTANCE, log,
                     error -> {
                         // Fail-stop (SPEC §6.4): after a failed fsync we no longer know what is on disk.
                         System.err.println("FATAL on " + config.id() + ": " + error);
                         error.printStackTrace();
                         Runtime.getRuntime().halt(1);
                     });
             NioTransport transport = NioTransport.start(config.id(), config.listen(), config.peers(), host)) {
            host.attach(transport);
            log.trace(banner());
            log.trace(config.id() + " listening on " + transport.localAddress() + ", data in "
                    + config.dataDir().toAbsolutePath() + ", cluster " + config.peers().keySet());
            host.addGroup(DATA_GROUP, config.peers().keySet(), RaftConfig.defaults());
            shutdownRequested.await();
            log.trace(config.id() + " shutting down");
        } finally {
            closed.countDown();
        }
    }

    /** One-line description of this build and the JVM running it. */
    static String banner() {
        return "RaftKV " + VERSION + " on Java " + Runtime.version()
                + " (" + System.getProperty("java.vendor") + ")";
    }

    /** Events that happen many times per second under load. */
    static boolean isNoisy(String event) {
        return event.contains(" commit ");
    }
}
