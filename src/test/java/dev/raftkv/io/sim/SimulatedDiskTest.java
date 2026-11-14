package dev.raftkv.io.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import dev.raftkv.io.DiskException;
import dev.raftkv.io.DiskFile;
import dev.raftkv.io.sim.SimulatedDisk.CrashMode;
import org.junit.jupiter.api.Test;

/** The simulated disk must behave like real hardware: only what was forced is guaranteed. */
class SimulatedDiskTest {

    private static final Path DIR = Path.of("/d");
    private static final Path FILE = DIR.resolve("f");

    private static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(SimulatedDisk disk, Path path) {
        try (DiskFile f = disk.open(path)) {
            return new String(f.readFully(0, (int) f.size()), StandardCharsets.UTF_8);
        }
    }

    /** A disk with a durable, durably-named file containing {@code content}. */
    private static SimulatedDisk diskWith(String content, long seed) {
        SimulatedDisk disk = new SimulatedDisk(new Random(seed));
        try (DiskFile f = disk.open(FILE)) {
            f.write(bytes(content), 0);
            f.force();
        }
        disk.syncDirectory(DIR);
        return disk;
    }

    @Test
    void forcedDataSurvivesAnyCrash() {
        for (CrashMode mode : CrashMode.values()) {
            SimulatedDisk disk = diskWith("durable", 1);
            disk.crash(mode);
            assertThat(read(disk, FILE)).as("%s", mode).isEqualTo("durable");
        }
    }

    @Test
    void unforcedWritesAreLostOrKeptDependingOnTheMode() {
        SimulatedDisk lose = diskWith("abc", 1);
        try (DiskFile f = lose.open(FILE)) {
            f.write(bytes("XYZ"), 3);
        }
        lose.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(lose, FILE)).isEqualTo("abc");

        SimulatedDisk keep = diskWith("abc", 1);
        try (DiskFile f = keep.open(FILE)) {
            f.write(bytes("XYZ"), 3);
        }
        keep.crash(CrashMode.KEEP_ALL);
        assertThat(read(keep, FILE)).isEqualTo("abcXYZ");
    }

    @Test
    void randomCrashesProduceLostKeptAndTornWrites() {
        Set<String> outcomes = new HashSet<>();
        for (long seed = 0; seed < 200; seed++) {
            SimulatedDisk disk = diskWith("abc", seed);
            try (DiskFile f = disk.open(FILE)) {
                f.write(bytes("XYZ"), 3);
            }
            disk.crash(CrashMode.RANDOM);
            outcomes.add(read(disk, FILE));
        }
        // lost, torn after 1 or 2 bytes, or kept
        assertThat(outcomes).isEqualTo(Set.of("abc", "abcX", "abcXY", "abcXYZ"));
    }

    @Test
    void aNewFileNameIsOnlyDurableAfterADirectorySync() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        try (DiskFile f = disk.open(FILE)) {
            f.write(bytes("data"), 0);
            f.force();                    // contents durable...
        }
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(disk.exists(FILE)).isFalse(); // ...but the name was never synced

        try (DiskFile f = disk.open(FILE)) {
            f.write(bytes("data"), 0);
            f.force();
        }
        disk.syncDirectory(DIR);
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(disk, FILE)).isEqualTo("data");
    }

    @Test
    void renameIsAtomicAndNeedsADirectorySyncToBeDurable() {
        SimulatedDisk disk = diskWith("old", 1);
        Path tmp = DIR.resolve("f.tmp");
        try (DiskFile f = disk.open(tmp)) {
            f.write(bytes("new"), 0);
            f.force();
        }
        disk.rename(tmp, FILE);
        assertThat(read(disk, FILE)).isEqualTo("new");
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(disk, FILE)).isEqualTo("old"); // the rename was undone

        try (DiskFile f = disk.open(tmp)) {
            f.write(bytes("new"), 0);
            f.force();
        }
        disk.rename(tmp, FILE);
        disk.syncDirectory(DIR);
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(disk, FILE)).isEqualTo("new");
        assertThat(disk.list(DIR)).isEqualTo(List.of("f"));
    }

    @Test
    void unforcedTruncationCanBeUndone() {
        SimulatedDisk disk = diskWith("abcdef", 1);
        try (DiskFile f = disk.open(FILE)) {
            f.truncate(2);
        }
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(disk, FILE)).isEqualTo("abcdef");
    }

    @Test
    void writesAfterATruncationLeaveNoStaleBytes() {
        SimulatedDisk disk = diskWith("abcdef", 1);
        try (DiskFile f = disk.open(FILE)) {
            f.truncate(2);
            f.write(bytes("Z"), 4); // leaves a hole at 2..3
            f.force();
        }
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(disk, FILE)).isEqualTo("ab\0\0Z");
    }

    @Test
    void handlesDieWithTheCrash() {
        SimulatedDisk disk = diskWith("x", 1);
        try (DiskFile f = disk.open(FILE)) {
            disk.crash(CrashMode.KEEP_ALL);
            assertThatThrownBy(f::size).isInstanceOf(DiskException.class).hasMessageContaining("crashed");
        }
    }

    @Test
    void crashAtOperationStopsExactlyThere() {
        SimulatedDisk disk = diskWith("x", 1);
        disk.crashAtOperation(2);
        try (DiskFile f = disk.open(FILE)) {
            f.write(bytes("1"), 1);                                  // operation 1: runs
            assertThatThrownBy(f::force).isInstanceOf(SimulatedDisk.SimulatedCrash.class); // operation 2: power cut
        }
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(disk, FILE)).isEqualTo("x");
    }

    @Test
    void flipBitCorruptsDurably() {
        SimulatedDisk disk = diskWith("a", 1);
        disk.flipBit(FILE, 0, 0);
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(read(disk, FILE)).isEqualTo("`"); // 'a' (0x61) with bit 0 flipped is 0x60
    }
}
