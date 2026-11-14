package dev.raftkv.raft.log;

/** Runs the {@link RaftLogContractTest} against {@link InMemoryLog}. */
class InMemoryLogContractTest extends RaftLogContractTest {

    @Override
    RaftLog newLog() {
        return new InMemoryLog();
    }

    @Override
    RaftLog reopen(RaftLog log) {
        return log; // nothing to recover
    }
}
