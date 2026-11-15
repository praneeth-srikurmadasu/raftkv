package dev.raftkv.raft.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;

import dev.raftkv.common.Bytes;
import dev.raftkv.io.sim.SimulatedDisk;
import dev.raftkv.io.sim.SimulatedDisk.CrashMode;
import org.junit.jupiter.api.Test;

class FileSnapshotStoreTest {

    private static final Path DIR = Path.of("/data/n1");

    private static Snapshot snapshot(long index, long term, int size) {
        byte[] data = new byte[size];
        new Random(index * 31 + term).nextBytes(data);
        return new Snapshot(index, term, Bytes.copyOf(data));
    }

    @Test
    void noSnapshotOnFirstBoot() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        assertThat(new FileSnapshotStore(disk, DIR).load()).isEqualTo(Optional.empty());
    }

    @Test
    void saveIsDurableTheMomentItReturns() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        Snapshot s = snapshot(500, 7, 10_000);
        new FileSnapshotStore(disk, DIR).save(s);
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(new FileSnapshotStore(disk, DIR).load()).isEqualTo(Optional.of(s));
    }

    @Test
    void anEmptyStateIsAValidSnapshot() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        Snapshot s = new Snapshot(1, 1, Bytes.EMPTY);
        new FileSnapshotStore(disk, DIR).save(s);
        assertThat(new FileSnapshotStore(disk, DIR).load()).isEqualTo(Optional.of(s));
    }

    /** Pull the plug before every disk operation of a save, with every crash mode: old or new, never a mix. */
    @Test
    void crashAtAnyStepLeavesTheOldOrTheNewSnapshot() {
        Snapshot older = snapshot(100, 3, 3_000);
        Snapshot newer = snapshot(250, 4, 5_000);
        int steps = countOperationsOfOneSave();
        int sawOld = 0;
        int sawNew = 0;
        for (long seed = 0; seed < 30; seed++) {
            for (CrashMode mode : CrashMode.values()) {
                for (int step = 1; step <= steps + 1; step++) {
                    SimulatedDisk disk = new SimulatedDisk(new Random(seed));
                    new FileSnapshotStore(disk, DIR).save(older);
                    disk.crashAtOperation(step);
                    try {
                        new FileSnapshotStore(disk, DIR).save(newer);
                    } catch (SimulatedDisk.SimulatedCrash expected) {
                        // power lost mid-save
                    }
                    disk.crash(mode);
                    Snapshot recovered = new FileSnapshotStore(disk, DIR).load().orElseThrow();
                    assertThat(recovered).as("seed %d, %s, step %d", seed, mode, step).isIn(older, newer);
                    if (recovered.equals(older)) {
                        sawOld++;
                    } else {
                        sawNew++;
                    }
                }
            }
        }
        assertThat(sawOld).isGreaterThan(0);
        assertThat(sawNew).isGreaterThan(0);
    }

    @Test
    void corruptionIsFatal() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        new FileSnapshotStore(disk, DIR).save(snapshot(9, 2, 400));
        disk.flipBit(DIR.resolve("snapshot"), 100, 5);
        assertThatThrownBy(() -> new FileSnapshotStore(disk, DIR).load())
                .isInstanceOf(CorruptStateException.class).hasMessageContaining("CRC");
    }

    private static int countOperationsOfOneSave() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        FileSnapshotStore store = new FileSnapshotStore(disk, DIR);
        store.save(snapshot(1, 1, 10));
        long before = disk.mutatingOperations();
        store.save(snapshot(2, 1, 10));
        return (int) (disk.mutatingOperations() - before);
    }
}
