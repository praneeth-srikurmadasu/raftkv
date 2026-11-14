package dev.raftkv.raft.log;

import java.nio.file.Path;
import java.util.Random;

import dev.raftkv.io.sim.SimulatedDisk;

/**
 * Runs the {@link RaftLogContractTest} against {@link SegmentedFileLog}. Every reopen goes through
 * a power failure that loses everything not synced, so the tests also prove durability. Tiny
 * segments make most tests span several segment files.
 */
class SegmentedFileLogContractTest extends RaftLogContractTest {

    private static final Path DIR = Path.of("/data/log");
    private final SimulatedDisk disk = new SimulatedDisk(new Random(1));

    @Override
    RaftLog newLog() {
        return SegmentedFileLog.open(disk, DIR, 512);
    }

    @Override
    RaftLog reopen(RaftLog log) {
        disk.crash(SimulatedDisk.CrashMode.LOSE_ALL_UNSYNCED);
        return SegmentedFileLog.open(disk, DIR, 512);
    }
}
