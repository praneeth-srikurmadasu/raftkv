package dev.raftkv;

/**
 * Entry point of a RaftKV node process.
 *
 * <p>Milestone 0: only prints the build and runtime information so we can
 * check that the toolchain works. Milestone 7 turns this into the real
 * node launcher (read config, start the RaftHost, open ports).
 */
public final class Main {

    /** Project version; kept in sync with pom.xml by hand until we need more. */
    public static final String VERSION = "0.1.0-SNAPSHOT";

    /** Lowest Java feature release we support (records, sealed types, virtual threads, pattern switch). */
    public static final int MIN_JAVA_FEATURE = 21;

    private Main() {
        // no instances
    }

    public static void main(String[] args) {
        int feature = Runtime.version().feature();
        if (feature < MIN_JAVA_FEATURE) {
            System.err.println("RaftKV needs Java " + MIN_JAVA_FEATURE + "+, but this is Java " + feature);
            System.exit(1);
        }
        System.out.println(banner());
    }

    /** One-line description of this build and the JVM running it. */
    static String banner() {
        return "RaftKV " + VERSION + " on Java " + Runtime.version()
                + " (" + System.getProperty("java.vendor") + ")";
    }
}
