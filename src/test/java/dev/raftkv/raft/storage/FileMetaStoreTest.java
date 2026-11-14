package dev.raftkv.raft.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Random;

import dev.raftkv.common.NodeId;
import dev.raftkv.io.sim.SimulatedDisk;
import dev.raftkv.io.sim.SimulatedDisk.CrashMode;
import org.junit.jupiter.api.Test;

class FileMetaStoreTest {

    private static final Path DIR = Path.of("/data/n1");

    @Test
    void firstBootIsTheInitialState() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        assertThat(new FileMetaStore(disk, DIR).load()).isEqualTo(PersistentState.INITIAL);
    }

    @Test
    void saveIsDurableTheMomentItReturns() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        new FileMetaStore(disk, DIR).save(new PersistentState(7, NodeId.of(3)));
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED); // the harshest crash
        assertThat(new FileMetaStore(disk, DIR).load()).isEqualTo(new PersistentState(7, NodeId.of(3)));
    }

    @Test
    void roundTripsWithoutAVote() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        FileMetaStore store = new FileMetaStore(disk, DIR);
        store.save(new PersistentState(12, null));
        assertThat(store.load()).isEqualTo(new PersistentState(12, null));
    }

    /**
     * Pull the plug before every single disk operation of a save (create tmp, write, fsync,
     * rename, dir fsync), with every crash mode and many seeds. After recovery the store must
     * hold either the complete old state or the complete new state. Never a mix, never garbage.
     */
    @Test
    void crashAtAnyStepLeavesTheOldOrTheNewStateNeverAMix() {
        PersistentState oldState = new PersistentState(4, NodeId.of(2));
        PersistentState newState = new PersistentState(5, NodeId.of(1));
        int stepsInASave = countOperationsOfOneSave();
        int sawOld = 0;
        int sawNew = 0;
        for (long seed = 0; seed < 50; seed++) {
            for (CrashMode mode : CrashMode.values()) {
                for (int step = 1; step <= stepsInASave + 1; step++) {
                    SimulatedDisk disk = new SimulatedDisk(new Random(seed));
                    new FileMetaStore(disk, DIR).save(oldState);
                    disk.crashAtOperation(step);
                    try {
                        new FileMetaStore(disk, DIR).save(newState);
                    } catch (SimulatedDisk.SimulatedCrash expected) {
                        // power lost mid-save
                    }
                    disk.crash(mode);
                    PersistentState recovered = new FileMetaStore(disk, DIR).load();
                    assertThat(recovered).as("seed %d, %s, crash before step %d", seed, mode, step).isIn(oldState, newState);
                    if (recovered.equals(oldState)) {
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

    /** Without the directory fsync, a crash can bring back the previous vote: the step matters. */
    @Test
    void theDirectoryFsyncIsWhatMakesTheRenameStick() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        FileMetaStore store = new FileMetaStore(disk, DIR);
        store.save(new PersistentState(4, NodeId.of(2)));
        int steps = countOperationsOfOneSave();
        disk.crashAtOperation(steps); // power is lost right before the final directory fsync
        try {
            store.save(new PersistentState(5, NodeId.of(1)));
        } catch (SimulatedDisk.SimulatedCrash expected) {
            // expected
        }
        disk.crash(CrashMode.LOSE_ALL_UNSYNCED);
        assertThat(new FileMetaStore(disk, DIR).load()).isEqualTo(new PersistentState(4, NodeId.of(2)));
    }

    @Test
    void corruptionIsFatalNotGuessed() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        new FileMetaStore(disk, DIR).save(new PersistentState(9, NodeId.of(1)));
        disk.flipBit(DIR.resolve("meta"), 6, 3); // inside the term
        assertThatThrownBy(() -> new FileMetaStore(disk, DIR).load())
                .isInstanceOf(CorruptStateException.class).hasMessageContaining("CRC");
    }

    private static int countOperationsOfOneSave() {
        SimulatedDisk disk = new SimulatedDisk(new Random(1));
        FileMetaStore store = new FileMetaStore(disk, DIR);
        store.save(new PersistentState(1, null));
        long before = disk.mutatingOperations();
        store.save(new PersistentState(2, null));
        return (int) (disk.mutatingOperations() - before);
    }
}
