package dev.raftkv;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

/**
 * Milestone 0 smoke tests. They prove the toolchain works before we write any Raft code:
 * JDK 21 language features compile, the JDK APIs we rely on exist, and all three
 * test libraries (JUnit 5, AssertJ, jqwik) are wired into the Maven build.
 */
class ToolchainTest {

    // A tiny sealed hierarchy, like the Message types we will write in Milestone 1.
    sealed interface Shape permits Circle, Square {}

    record Circle(double radius) implements Shape {}

    record Square(double side) implements Shape {}

    /** Exhaustive pattern-matching switch: no default branch needed for a sealed type. */
    static double area(Shape shape) {
        return switch (shape) {
            case Circle c -> Math.PI * c.radius() * c.radius();
            case Square s -> s.side() * s.side();
        };
    }

    @Test
    void runsOnJava21OrNewer() {
        assertThat(Runtime.version().feature()).isGreaterThanOrEqualTo(Main.MIN_JAVA_FEATURE);
    }

    @Test
    void sealedTypesRecordsAndPatternSwitchWork() {
        assertThat(area(new Square(3))).isEqualTo(9.0);
        assertThat(area(new Circle(1))).isCloseTo(Math.PI, org.assertj.core.data.Offset.offset(1e-12));
        // Records give us value equality for free.
        assertThat(new Square(2)).isEqualTo(new Square(2));
    }

    @Test
    void virtualThreadsWork() throws InterruptedException {
        AtomicReference<String> seen = new AtomicReference<>();
        Thread t = Thread.ofVirtual().start(() -> seen.set(Thread.currentThread().isVirtual() ? "virtual" : "platform"));
        t.join();
        assertThat(seen.get()).isEqualTo("virtual");
    }

    @Test
    void crc32cMatchesKnownCheckValue() {
        // "123456789" is the standard check input; CRC-32C of it is 0xE3069283.
        CRC32C crc = new CRC32C();
        crc.update("123456789".getBytes(StandardCharsets.US_ASCII));
        assertThat(crc.getValue()).isEqualTo(0xE3069283L);
    }

    @Test
    void bannerMentionsVersion() {
        assertThat(Main.banner()).contains("RaftKV " + Main.VERSION);
    }

    /** jqwik runs this with 1,000 random lists by default. */
    @Property
    void reversingTwiceGivesTheOriginalList(@ForAll List<Integer> original) {
        List<Integer> copy = new ArrayList<>(original);
        Collections.reverse(copy);
        Collections.reverse(copy);
        assertThat(copy).isEqualTo(original);
    }
}
