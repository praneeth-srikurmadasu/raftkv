package dev.raftkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.sim.SimulationException;

/**
 * One randomized chaos run for log replication: the election chaos of {@link ElectionChaos}
 * plus a client that keeps proposing commands the whole time.
 *
 * <p>During the run, all election and replication invariants are checked after every event,
 * and every replica's state machine checks State Machine Safety on every apply. At the end,
 * after healing and settling:
 * <ul>
 *   <li>a final "marker" command must commit and be applied on every node within 60 s of
 *       virtual time (liveness),</li>
 *   <li>all nodes must hold identical logs up to the marker and have applied the identical
 *       sequence of commands,</li>
 *   <li>every command whose proposal was <b>acknowledged</b> must be in the final log at the
 *       index it was acknowledged with (acknowledged writes are never lost).</li>
 * </ul>
 */
final class ReplicationChaos {

    private ReplicationChaos() {
    }

    record Result(long seed, int size, Optional<String> failure, long committed, int acknowledged) {
        boolean passed() {
            return failure.isEmpty();
        }
    }

    static Result run(long seed, RaftConfig config) {
        Random r = new Random(seed);
        int size = r.nextBoolean() ? 3 : 5;
        // Small batches stress backtracking and partial matches; large ones exercise bulk catch-up.
        int batch = r.nextInt(3) == 0 ? 1 + r.nextInt(3) : 1 + r.nextInt(64);
        SimulatedCluster c = new SimulatedCluster(seed, size, config.withMaxEntriesPerAppend(batch));
        Map<Long, String> acknowledged = new TreeMap<>();
        try {
            boolean partitions = r.nextInt(4) != 0;
            boolean crashes = r.nextInt(4) != 0;
            c.net.setLatency(Duration.ofMillis(1), Duration.ofMillis(5 + r.nextInt(26)));
            c.net.setDropRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.15);
            c.net.setDuplicateRate(r.nextBoolean() ? 0 : r.nextDouble() * 0.1);
            c.startAll();

            // The client: every 5-40 ms, propose a unique command to SOME node that believes it is
            // leader. Like real clients with out-of-date leader knowledge, it often hits a leader
            // that has already been replaced. Those writes leave divergent, uncommitted entries
            // behind, exactly the situation the trickiest replication rules exist for.
            Random client = c.sim.forkRandom();
            int[] counter = {0};
            Runnable[] tick = new Runnable[1];
            boolean[] clientOn = {true};
            tick[0] = () -> {
                if (!clientOn[0]) {
                    return;
                }
                List<RaftNode> leaders = c.leaders();
                RaftNode leader = leaders.isEmpty() ? null : leaders.get(client.nextInt(leaders.size()));
                if (leader != null) {
                    String cmd = "c" + (counter[0]++);
                    leader.propose(Bytes.ofUtf8(cmd)).thenAccept(res -> {
                        String previous = acknowledged.putIfAbsent(res.index(), cmd);
                        if (previous != null) {
                            throw new AssertionError("index " + res.index() + " acknowledged twice: " + previous + ", " + cmd);
                        }
                    });
                }
                c.sim.schedule(Duration.ofMillis(5 + client.nextInt(36)), tick[0]);
            };
            c.sim.execute(tick[0]);

            Random nemesis = c.sim.forkRandom();
            long chaosEnd = Duration.ofSeconds(10).toNanos();
            while (c.sim.now() < chaosEnd) {
                c.run(Duration.ofMillis(100 + nemesis.nextInt(501)));
                ElectionChaos.act(c, nemesis, partitions, crashes);
            }

            // Recovery.
            clientOn[0] = false;
            c.net.heal();
            for (NodeId id : c.crashedNodes()) {
                c.restart(id);
            }
            c.net.setDropRate(0);
            c.net.setDuplicateRate(0);
            c.run(Duration.ofSeconds(2));
            RaftNode leader = c.awaitLeader(Duration.ofSeconds(10));
            CompletableFuture<CommitResult> marker = leader.propose(Bytes.ofUtf8("marker"));
            c.sim.runUntil(marker::isDone, Duration.ofSeconds(5));
            if (!marker.isDone() || marker.isCompletedExceptionally()) {
                throw new AssertionError("marker did not commit on a healthy network: " + c.runningNodes());
            }
            long markerIndex = marker.join().index();
            // A follower that was cut off for a long time may be hundreds of entries behind, and
            // with tiny batches it catches up one batch per round trip. Allow plenty of
            // (virtual, nearly free) time: the property is "eventually", not "fast".
            c.awaitAppliedEverywhere(markerIndex, Duration.ofSeconds(60));

            // Identical logs and identical applied sequences up to the marker.
            List<LogEntry> reference = c.log(leader.id()).entriesFrom(1, (int) markerIndex);
            for (RaftNode n : c.runningNodes()) {
                if (!c.log(n.id()).entriesFrom(1, (int) markerIndex).equals(reference)) {
                    throw new AssertionError("logs differ up to the marker: " + n.id() + " vs leader " + leader.id());
                }
            }
            List<RecordingStateMachine.Applied> appliedRef = upTo(c.stateMachine(leader.id()).applied(), markerIndex);
            for (RaftNode n : c.runningNodes()) {
                if (!upTo(c.stateMachine(n.id()).applied(), markerIndex).equals(appliedRef)) {
                    throw new AssertionError("applied sequences differ: " + n.id() + " vs leader " + leader.id());
                }
            }
            // Acknowledged writes are durable.
            for (Map.Entry<Long, String> ack : acknowledged.entrySet()) {
                Bytes inLog = c.log(leader.id()).entryAt(ack.getKey()).data();
                if (!inLog.equals(Bytes.ofUtf8(ack.getValue()))) {
                    throw new AssertionError("acknowledged write " + ack.getValue() + " at index " + ack.getKey()
                            + " was lost (log has " + inLog.toUtf8() + ")");
                }
            }
            return new Result(seed, size, Optional.empty(), markerIndex, acknowledged.size());
        } catch (AssertionError | SimulationException | IllegalStateException e) {
            String message = e.getMessage() + (e.getCause() != null ? " / cause: " + e.getCause().getMessage() : "");
            return new Result(seed, size, Optional.of(message), c.replicationInvariants.maxCommitted(), acknowledged.size());
        }
    }

    private static List<RecordingStateMachine.Applied> upTo(List<RecordingStateMachine.Applied> applied, long index) {
        List<RecordingStateMachine.Applied> out = new ArrayList<>();
        for (RecordingStateMachine.Applied a : applied) {
            if (a.index() <= index) {
                out.add(a);
            }
        }
        return out;
    }
}
