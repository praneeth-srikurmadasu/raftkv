package dev.raftkv.raft;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.Cancellable;
import dev.raftkv.common.Clock;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.common.Scheduler;
import dev.raftkv.common.Tracer;
import dev.raftkv.protocol.AppendEntries;
import dev.raftkv.protocol.AppendEntriesReply;
import dev.raftkv.protocol.InstallSnapshot;
import dev.raftkv.protocol.InstallSnapshotReply;
import dev.raftkv.protocol.Message;
import dev.raftkv.protocol.RaftMessage;
import dev.raftkv.protocol.RequestVote;
import dev.raftkv.protocol.RequestVoteReply;
import dev.raftkv.protocol.TimeoutNow;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.raft.log.RaftLog;
import dev.raftkv.raft.storage.InMemorySnapshotStore;
import dev.raftkv.raft.storage.MetaStore;
import dev.raftkv.raft.storage.PersistentState;
import dev.raftkv.raft.storage.Snapshot;
import dev.raftkv.raft.storage.SnapshotStore;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * One server's replica of one Raft group.
 *
 * <p><b>Scope so far:</b> leader election (Milestone 2) and log replication (Milestone 3):
 * AppendEntries with the full Figure 2 receiver rules, fast backtracking, the commit rule that
 * only counts replicas for entries of the current term (§5.4.2, Figure 8), a no-op entry on
 * election, and applying committed entries to the {@link StateMachine} in order. Milestone 8
 * adds snapshots (§7): once enough applied entries pile up, the state machine is snapshotted,
 * the snapshot saved durably, and the log prefix it covers discarded; a follower too far behind
 * for the remaining log gets the snapshot in chunks (InstallSnapshot, Figure 13).
 *
 * <p><b>Threading.</b> Every public method must be called on this node's {@link Scheduler}
 * (its event loop). The node never blocks and never creates threads, which is what makes it
 * deterministic under simulation (SPEC §3.2).
 *
 * <p><b>Durability.</b> {@code currentTerm} and {@code votedFor} are saved through the
 * {@link MetaStore} before any message that depends on them is sent, and a follower fsyncs its
 * log before acknowledging entries (Figure 2: "updated on stable storage before responding to
 * RPCs"). A leader fsyncs its own new entries in a scheduled flush (group commit) while it
 * replicates them, and counts its own copy toward a majority only once it is durable.
 *
 * <p>Comments of the form {@code // Fig.2 ...} point at the exact rule of Figure 2 of the
 * extended Raft paper that the line implements.
 */
public final class RaftNode {

    private final NodeId self;
    private final GroupId group;
    /** Membership when neither the log nor a snapshot holds one (a brand-new group). */
    private final Membership bootstrapMembership;
    private final RaftConfig config;
    private final Scheduler scheduler;
    private final Clock clock;
    private final Random random;
    private final Transport transport;
    private final RaftLog log;
    private final MetaStore metaStore;
    private final StateMachine stateMachine;
    private final SnapshotStore snapshotStore;
    private final Tracer tracer;

    // ---- Persistent state on all servers (Figure 2); the log is the third piece.
    private long currentTerm;
    private NodeId votedFor;

    // ---- Volatile state.
    private Role role = Role.FOLLOWER;
    private NodeId leaderId;
    private boolean running;
    /** Fig.2: index of highest log entry known to be committed (initialized to 0, increases monotonically). */
    private long commitIndex;
    /** Fig.2: index of highest log entry applied to state machine (initialized to 0, increases monotonically). */
    private long lastApplied;
    /** Highest log index known to be on stable storage (the leader counts only this much of its own log). */
    private long durableIndex;
    /** A flush (log fsync) is already scheduled; more appends will ride along with it (group commit). */
    private boolean flushScheduled;
    /**
     * Follower group commit: success replies that promise durability, waiting for the next
     * flush. Every AppendEntries that arrives before the flush runs shares one fsync.
     */
    private final List<PendingAck> pendingAcks = new ArrayList<>();

    private record PendingAck(NodeId to, long term, long matchIndex, long round) {}
    private long logSyncs;

    // ---- Leader state (Fig.2 "volatile state on leaders"); null unless leader.
    private LeaderState leaderState;
    /** Proposals waiting for their entry to be applied: index -> who is waiting. */
    private final Map<Long, Pending> pending = new TreeMap<>();

    private record Pending(long term, CompletableFuture<CommitResult> future) {}

    // ---- Candidate state: who answered, who granted.
    private final Set<NodeId> votesGranted = new TreeSet<>();
    private final Set<NodeId> votesResponded = new TreeSet<>();

    // ---- Timers.
    private Cancellable electionTimer;
    private Cancellable heartbeatTimer;
    private Cancellable voteRetryTimer;

    /** When we last heard from a valid leader of the current term (for leader stickiness). */
    private long lastLeaderContactNanos;
    private boolean heardFromLeader;

    // ---- Snapshots.
    /** The snapshot our log was compacted to (what we send to followers that are too far behind). */
    private Snapshot latestSnapshot;
    /** A snapshot arriving from the leader in chunks. */
    private IncomingSnapshot incoming;
    private long snapshotsTaken;
    private long snapshotsInstalled;

    private static final class IncomingSnapshot {
        final long index;
        final long term;
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        IncomingSnapshot(long index, long term) {
            this.index = index;
            this.term = term;
        }
    }

    // ---- Counters for tests and metrics.
    private long electionsStarted;
    private long preVotesStarted;

    // ---- Membership (thesis ch. 4). The configuration in effect is the latest one in the log,
    // committed or not; if the log holds none, the one in the snapshot; else the bootstrap one.
    private Membership snapshotMembership;
    private final TreeMap<Long, Membership> configEntries = new TreeMap<>();
    private Membership membership;

    // ---- Pre-vote (thesis §9.6).
    private final Set<NodeId> preVotesGranted = new TreeSet<>();
    private final Set<NodeId> preVotesResponded = new TreeSet<>();

    // ---- Check-quorum and heartbeat rounds (ReadIndex, leases).
    private Cancellable checkQuorumTimer;
    private long round;
    private long confirmedRound;
    private final TreeMap<Long, Long> roundSentNanos = new TreeMap<>();
    private long leaseExpiryNanos = Long.MIN_VALUE;
    private final List<PendingRead> pendingReads = new ArrayList<>();
    private boolean roundScheduled;

    /** A linearizable read waiting for (1) a current-term commit, (2) leadership confirmation, (3) apply. */
    private static final class PendingRead {
        final CompletableFuture<Long> future;
        final ReadMode mode;
        long readIndex = -1;  // set once the leader has committed an entry of its term
        long round = -1;      // the heartbeat round that must be confirmed (READ_INDEX)
        boolean confirmed;

        PendingRead(CompletableFuture<Long> future, ReadMode mode) {
            this.future = future;
            this.mode = mode;
        }
    }

    // ---- Membership changes and leadership transfer (leader only).
    private final Map<NodeId, CompletableFuture<Void>> promoteWhenCaughtUp = new TreeMap<>();
    /** Changes requested before an entry of this leader's term committed: run once one has. */
    private final List<Runnable> afterTermCommit = new ArrayList<>();
    private boolean timeoutNowSent;
    private NodeId transferTarget;
    private long transferDeadlineNanos;
    private CompletableFuture<Void> transferFuture;
    private boolean transferElection; // our current election was started by TimeoutNow

    private RaftNode(Builder b) {
        this.self = Objects.requireNonNull(b.self, "self");
        this.group = Objects.requireNonNull(b.group, "group");
        this.bootstrapMembership = Membership.of(Objects.requireNonNull(b.voters, "voters"));
        this.membership = bootstrapMembership;
        this.config = Objects.requireNonNull(b.config, "config");
        this.scheduler = Objects.requireNonNull(b.scheduler, "scheduler");
        this.clock = Objects.requireNonNull(b.clock, "clock");
        this.random = Objects.requireNonNull(b.random, "random");
        this.transport = Objects.requireNonNull(b.transport, "transport");
        this.log = Objects.requireNonNull(b.log, "log");
        this.metaStore = Objects.requireNonNull(b.metaStore, "metaStore");
        this.stateMachine = b.stateMachine == null ? new NoState() : b.stateMachine;
        this.snapshotStore = b.snapshotStore == null ? new InMemorySnapshotStore() : b.snapshotStore;
        this.tracer = b.tracer == null ? Tracer.NOOP : b.tracer;
        if (!transport.self().equals(self)) {
            throw new IllegalArgumentException("transport belongs to " + transport.self() + ", not " + self);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The state machine of a node built without one: applies nothing, snapshots to nothing. */
    private static final class NoState implements StateMachine {
        @Override
        public Bytes apply(long index, Bytes command) {
            return Bytes.EMPTY;
        }

        @Override
        public Bytes snapshot() {
            return Bytes.EMPTY;
        }

        @Override
        public void restore(Bytes snapshot) {
            // nothing to restore
        }
    }

    // =====================================================================================
    // Lifecycle
    // =====================================================================================

    /** Loads persistent state and starts as a follower (§5.2: "When servers start up, they begin as followers"). */
    public void start() {
        if (running) {
            throw new IllegalStateException(self + " already started");
        }
        PersistentState persisted = metaStore.load();
        currentTerm = persisted.currentTerm();
        votedFor = persisted.votedFor();
        // The state machine is rebuilt from the latest snapshot plus the log after it. commitIndex
        // and lastApplied are volatile (Figure 2), but everything a snapshot covers was committed
        // and applied, so they restart at its index instead of 0.
        latestSnapshot = snapshotStore.load().orElse(null);
        incoming = null;
        snapshotMembership = null;
        commitIndex = 0;
        lastApplied = 0;
        if (latestSnapshot != null) {
            long si = latestSnapshot.lastIncludedIndex();
            long st = latestSnapshot.lastIncludedTerm();
            if (si < log.snapshotIndex()) {
                throw new IllegalStateException("corrupt state: log starts after " + log.snapshotIndex()
                        + " but the snapshot only covers up to " + si);
            }
            if (log.contains(si) && (si == log.snapshotIndex() || log.termAt(si) == st)) {
                log.compactPrefix(si, st); // keep the entries after the snapshot
            } else {
                log.reset(si, st);         // the log is behind the snapshot, or conflicts with it
            }
            restoreFromSnapshotData(latestSnapshot.data());
            commitIndex = si;
            lastApplied = si;
        } else if (log.snapshotIndex() > 0) {
            throw new IllegalStateException("corrupt state: log starts after " + log.snapshotIndex()
                    + " but there is no snapshot");
        }
        if (currentTerm < log.lastTerm()) {
            throw new IllegalStateException("corrupt state: currentTerm " + currentTerm
                    + " is older than the last log term " + log.lastTerm());
        }
        configEntries.clear();
        for (long i = log.snapshotIndex() + 1; i <= log.lastIndex(); i++) {
            LogEntry e = log.entryAt(i);
            if (e.type() == EntryType.CONFIGURATION) {
                configEntries.put(i, Membership.decode(e.data()));
            }
        }
        membership = currentMembership();
        running = true;
        role = membership.isVoter(self) ? Role.FOLLOWER : Role.LEARNER;
        durableIndex = log.lastIndex(); // whatever survived the restart is on disk
        flushScheduled = false;
        pendingAcks.clear();
        trace("started (votedFor=" + votedFor + ", last log " + log.lastIndex() + "@t" + log.lastTerm()
                + (latestSnapshot != null ? ", snapshot " + latestSnapshot.lastIncludedIndex() : "") + ", " + membership + ")");
        resetElectionTimer();
    }

    /** Stops all activity; volatile state is lost, like a crash. Persistent state stays in the stores. */
    public void stop() {
        running = false;
        cancel(electionTimer);
        cancel(heartbeatTimer);
        cancel(voteRetryTimer);
        cancel(checkQuorumTimer);
        pendingAcks.clear();
        failPending();
        failLeaderWork(new NotLeaderException(null));
        trace("stopped");
    }

    // =====================================================================================
    // Client proposals
    // =====================================================================================

    /**
     * Asks the group to replicate {@code command}. Only the leader accepts: it appends the entry
     * to its own log and starts replicating it at once.
     *
     * <p>The future completes when the entry is <b>committed and applied</b>, with the state
     * machine's result. It fails with {@link NotLeaderException} if this node is not the leader,
     * or stops being leader before the entry commits.
     */
    public CompletableFuture<CommitResult> propose(Bytes command) {
        Objects.requireNonNull(command, "command");
        if (!running || role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(running ? leaderId : null));
        }
        if (transferTarget != null) {
            // Leadership transfer in progress: stop taking new work (thesis §3.10).
            return CompletableFuture.failedFuture(new NotLeaderException(transferTarget));
        }
        // Fig.2 Leaders: "If command received from client: append entry to local log, respond
        // after entry applied to state machine (§5.3)".
        return appendAsLeader(EntryType.COMMAND, command);
    }

    private CompletableFuture<CommitResult> appendAsLeader(EntryType type, Bytes data) {
        LogEntry entry = new LogEntry(log.lastIndex() + 1, currentTerm, type, data);
        appendToLog(List.of(entry));
        CompletableFuture<CommitResult> future = new CompletableFuture<>();
        pending.put(entry.index(), new Pending(entry.term(), future));
        scheduleFlush();  // our own fsync runs in the background ...
        replicateToAll(); // ... while followers already receive the entry (thesis §10.2.1)
        return future;
    }

    // =====================================================================================
    // Incoming messages
    // =====================================================================================

    /** Handles one message from a peer. Must be called on this node's event loop. */
    public void handle(Envelope envelope) {
        if (!running) {
            return;
        }
        if (!(envelope.message() instanceof RaftMessage message)) {
            return; // client traffic is handled by the server layer above Raft (KVServer)
        }
        NodeId from = envelope.from();
        if (!envelope.group().equals(group) || from.equals(self)) {
            trace("ignored message from " + from + " for " + envelope.group());
            return;
        }
        // Messages from servers outside our membership are accepted: a new server hears from the
        // leader before it has the configuration that adds it, and votes from non-voters are
        // simply not counted.

        // Pre-vote requests change no state on the voter: answered here, before term adoption.
        if (message instanceof RequestVote rv && rv.preVote()) {
            onPreVoteRequest(from, rv);
            return;
        }

        // Thesis §4.2.3 leader stickiness: if we believe a leader is alive (we are it, or we
        // heard from it within the minimum election timeout), a RequestVote is a disruption.
        // Reject it WITHOUT adopting its term, so a node that was cut off and comes back with
        // a high term cannot depose a healthy leader. Leadership transfer bypasses this.
        if (message instanceof RequestVote rv && !rv.leadershipTransfer() && believesLeaderIsAlive()) {
            trace("rejected vote for " + from + " (t" + rv.term() + "): current leader is alive");
            send(from, new RequestVoteReply(currentTerm, false, false));
            return;
        }
        // A granted pre-vote reply carries the voter's term, which may be the term we are about
        // to campaign in; it is not a sign that we are behind.
        boolean grantedPreVote = message instanceof RequestVoteReply r && r.preVote() && r.voteGranted();

        // Fig.2 All Servers: "If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)".
        if (message.term() > currentTerm && !grantedPreVote
                && !(role == Role.LEADER && config.has(Bug.LEADER_IGNORES_HIGHER_TERM))) {
            stepDown(message.term(), "saw higher term from " + from);
        }

        switch (message) {
            case RequestVote m -> onRequestVote(from, m);
            case RequestVoteReply m -> onRequestVoteReply(from, m);
            case AppendEntries m -> onAppendEntries(from, m);
            case AppendEntriesReply m -> onAppendEntriesReply(from, m);
            case InstallSnapshot m -> onInstallSnapshot(from, m);
            case InstallSnapshotReply m -> onInstallSnapshotReply(from, m);
            case TimeoutNow m -> onTimeoutNow(from, m);
        }
    }

    // ---------------------------------------------------------------- Pre-vote (receiver)

    /**
     * Thesis §9.6: "would you vote for me?". Yes only if the candidate's log is at least as up to
     * date as ours and we haven't heard from a leader within the minimum election timeout. Changes
     * nothing here: no term, no vote, no timer.
     */
    private void onPreVoteRequest(NodeId from, RequestVote rv) {
        boolean grant = rv.candidateId().equals(from)
                && rv.term() >= currentTerm
                && (candidateLogIsUpToDate(rv) || config.has(Bug.VOTE_WITHOUT_LOG_CHECK))
                && (rv.leadershipTransfer() || !believesLeaderIsAlive());
        trace((grant ? "granted" : "denied") + " pre-vote to " + from + " for t" + rv.term());
        if (!grant && config.has(Bug.RESET_TIMER_ON_REJECTED_VOTE)) {
            resetElectionTimer(); // BUG (see Bug.RESET_TIMER_ON_REJECTED_VOTE)
        }
        send(from, new RequestVoteReply(currentTerm, grant, true));
    }

    // ---------------------------------------------------------------- RequestVote (receiver)

    private void onRequestVote(NodeId from, RequestVote rv) {
        if (!rv.candidateId().equals(from)) {
            trace("ignored RequestVote from " + from + " claiming to be " + rv.candidateId());
            return;
        }
        boolean grant;
        String reason;
        if (rv.term() < currentTerm) {
            // Fig.2 RequestVote rule 1: "Reply false if term < currentTerm (§5.1)".
            grant = false;
            reason = "stale term t" + rv.term();
        } else if (votedFor != null && !votedFor.equals(rv.candidateId()) && !config.has(Bug.VOTE_TWICE_IN_TERM)) {
            // Fig.2 RequestVote rule 2, first half: "If votedFor is null or candidateId ...".
            grant = false;
            reason = "already voted for " + votedFor;
        } else if (!candidateLogIsUpToDate(rv) && !config.has(Bug.VOTE_WITHOUT_LOG_CHECK)) {
            // Fig.2 RequestVote rule 2, second half: "... and candidate's log is at least as
            // up-to-date as receiver's log, grant vote (§5.2, §5.4)".
            grant = false;
            reason = "candidate log " + rv.lastLogIndex() + "@t" + rv.lastLogTerm()
                    + " is behind ours " + log.lastIndex() + "@t" + log.lastTerm();
        } else {
            grant = true;
            reason = "";
        }

        if (grant) {
            votedFor = rv.candidateId();
            persist(); // durable BEFORE the reply leaves (Figure 2, persistent state)
            // Fig.2 Followers: the election timer restarts when we grant a vote.
            resetElectionTimer();
            trace("granted vote to " + from);
        } else {
            trace("denied vote to " + from + ": " + reason);
            if (config.has(Bug.RESET_TIMER_ON_REJECTED_VOTE)) {
                resetElectionTimer(); // BUG: only a GRANTED vote restarts the timer
            }
        }
        send(from, new RequestVoteReply(currentTerm, grant, false));
    }

    /**
     * §5.4.1: "If the logs have last entries with different terms, then the log with the later
     * term is more up-to-date. If the logs end with the same term, then whichever log is
     * longer is more up-to-date."
     */
    private boolean candidateLogIsUpToDate(RequestVote rv) {
        long myLastTerm = log.lastTerm();
        if (rv.lastLogTerm() != myLastTerm) {
            return rv.lastLogTerm() > myLastTerm;
        }
        return rv.lastLogIndex() >= log.lastIndex();
    }

    // ---------------------------------------------------------------- RequestVote (candidate side)

    private void onRequestVoteReply(NodeId from, RequestVoteReply reply) {
        // Ignore replies that belong to another election: we are no longer a candidate, the
        // reply is from an older term, or it answers a pre-vote (Milestone 11).
        if (reply.preVote()) {
            if (role != Role.PRE_CANDIDATE || reply.term() > currentTerm + 1) {
                return;
            }
            preVotesResponded.add(from);
            if (reply.voteGranted()) {
                preVotesGranted.add(from);
                if (countVoters(preVotesGranted) >= membership.quorum()) {
                    trace("won pre-vote with " + preVotesGranted);
                    startElection(false);
                }
            }
            return;
        }
        if (role != Role.CANDIDATE || reply.term() != currentTerm) {
            return;
        }
        votesResponded.add(from);
        if (reply.voteGranted()) {
            votesGranted.add(from);
            trace("got vote from " + from + " (" + countVoters(votesGranted) + "/" + membership.voters().size() + ")");
            // Fig.2 Candidates: "If votes received from majority of servers: become leader".
            if (countVoters(votesGranted) >= membership.quorum()) {
                becomeLeader();
            }
        }
    }

    // ---------------------------------------------------------------- AppendEntries (receiver)

    private void onAppendEntries(NodeId from, AppendEntries ae) {
        if (ae.term() < currentTerm) {
            // Fig.2 AppendEntries rule 1: "Reply false if term < currentTerm (§5.1)". Our term
            // in the reply tells the stale leader to step down.
            send(from, new AppendEntriesReply(currentTerm, false, 0, 0, 0, ae.round()));
            return;
        }
        if (ae.term() > currentTerm) {
            return; // only reachable with Bug.LEADER_IGNORES_HIGHER_TERM
        }
        if (!acceptLeader(from)) {
            return;
        }

        long prevIndex = ae.prevLogIndex();
        if (prevIndex < log.snapshotIndex()) {
            // The message starts inside our snapshot (it is stale, or the leader doesn't know how
            // far we are). Everything up to our commitIndex is committed, so it is in the leader's
            // log too and durable here: report it as matching (as etcd does), and the leader
            // continues from there. A rejection would not work: a leader never raises nextIndex
            // because of a rejection, so it could keep sending from below our snapshot forever.
            send(from, new AppendEntriesReply(currentTerm, true, commitIndex, 0, 0, ae.round()));
            return;
        }
        // Fig.2 AppendEntries rule 2: "Reply false if log doesn't contain an entry at
        // prevLogIndex whose term matches prevLogTerm (§5.3)".
        if (!log.contains(prevIndex)) {
            // Fast backtracking (§5.3): our log is too short; retry from just past our end.
            send(from, new AppendEntriesReply(currentTerm, false, 0, log.lastIndex() + 1, 0, ae.round()));
            return;
        }
        long termAtPrev = log.termAt(prevIndex);
        if (termAtPrev != ae.prevLogTerm()) {
            // Fast backtracking: report the conflicting term and the first index we hold for it,
            // so the leader can skip the whole term in one round trip.
            send(from, new AppendEntriesReply(currentTerm, false, 0, firstIndexOfTerm(termAtPrev, prevIndex), termAtPrev, ae.round()));
            return;
        }

        boolean changed = appendNewEntries(ae);
        long lastNewIndex = prevIndex + ae.entries().size();
        // Figure 2: persistent state is "updated on stable storage before responding to RPCs".
        // The success reply below promises the leader that everything up to lastNewIndex
        // survives a crash. Sync if the log changed, and ALSO if the reply covers entries we
        // appended earlier but never synced (e.g. as a short-lived leader whose flush hadn't run).
        boolean mustSync = changed || lastNewIndex > durableIndex;
        if (mustSync && config.has(Bug.REPLY_BEFORE_FSYNC)) {
            durableIndex = log.lastIndex(); // BUG: promise durability without syncing
            mustSync = false;
        }
        durableIndex = Math.min(durableIndex, log.lastIndex());
        // Fig.2 AppendEntries rule 5: "If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)". NOT the last index of our log: entries
        // beyond lastNewIndex were not checked against the leader and may be stale.
        long bound = config.has(Bug.COMMIT_INDEX_FROM_LAST_LOG_INDEX) ? log.lastIndex() : lastNewIndex;
        if (ae.leaderCommit() > commitIndex) {
            long newCommit = Math.min(ae.leaderCommit(), bound);
            if (newCommit > commitIndex) {
                commitIndex = newCommit;
                applyCommitted();
            }
        }
        if (mustSync) {
            // Group commit (SPEC §3.7): the reply waits for the next flush, which covers every
            // AppendEntries that arrived before it (pipelined batches share one fsync).
            pendingAcks.add(new PendingAck(from, currentTerm, lastNewIndex, ae.round()));
            scheduleFlush();
        } else {
            send(from, new AppendEntriesReply(currentTerm, true, lastNewIndex, 0, 0, ae.round()));
        }
    }

    /**
     * A message from the leader of our current term arrived (AppendEntries or InstallSnapshot):
     * follow it and restart the election timer. Returns false if the message must be dropped.
     */
    private boolean acceptLeader(NodeId from) {
        if (role == Role.LEADER) {
            // Election Safety (§5.2): at most one leader per term. Seeing another one is a bug.
            throw new IllegalStateException(self + " is leader of term " + currentTerm
                    + " but received a leader message from " + from + " for the same term");
        }
        if (role == Role.CANDIDATE || role == Role.PRE_CANDIDATE) {
            // Fig.2 Candidates: "If AppendEntries RPC received from new leader: convert to follower".
            becomeFollower("leader " + from + " exists for t" + currentTerm);
        }
        if (!from.equals(leaderId)) {
            leaderId = from;
            trace("following leader " + from);
        }
        heardFromLeader = true;
        lastLeaderContactNanos = clock.nanos();
        // Fig.2 Followers: hearing from the current leader restarts the election timer.
        resetElectionTimer();
        return true;
    }

    /** Fig.2 AppendEntries rules 3 and 4. Returns true if the log changed. */
    private boolean appendNewEntries(AppendEntries ae) {
        List<LogEntry> entries = ae.entries();
        if (config.has(Bug.TRUNCATE_ON_STALE_APPEND)) {
            if (log.lastIndex() > ae.prevLogIndex()) {
                truncateLog(ae.prevLogIndex() + 1);
            }
            appendToLog(entries);
            return true;
        }
        for (int i = 0; i < entries.size(); i++) {
            LogEntry e = entries.get(i);
            if (log.contains(e.index())) {
                if (log.termAt(e.index()) == e.term()) {
                    continue; // already have it. Never truncate on a match: this message may be stale.
                }
                // Rule 3: "If an existing entry conflicts with a new one (same index but different
                // terms), delete the existing entry and all that follow it (§5.3)".
                if (e.index() <= commitIndex) {
                    throw new IllegalStateException(self + " asked to overwrite committed entry " + e.index());
                }
                trace("truncating log from " + e.index() + " (had t" + log.termAt(e.index()) + ", leader has t" + e.term() + ")");
                truncateLog(e.index());
            }
            // Rule 4: "Append any new entries not already in the log".
            appendToLog(entries.subList(i, entries.size()));
            return true;
        }
        return false;
    }

    /**
     * Schedules one fsync of our own log on the event loop. Every append made before it runs is
     * covered by the same fsync: that's group commit (SPEC §3.7). Until it completes, the leader
     * does not count its own copy of those entries toward a majority.
     */
    private void scheduleFlush() {
        if (flushScheduled) {
            return;
        }
        flushScheduled = true;
        scheduler.execute(this::flush);
    }

    private void flush() {
        flushScheduled = false;
        if (!running) {
            return;
        }
        if (!config.has(Bug.REPLY_BEFORE_FSYNC)) {
            log.sync();
            logSyncs++;
        }
        durableIndex = log.lastIndex();
        if (role == Role.LEADER) {
            advanceCommitIndex();
        }
        if (!pendingAcks.isEmpty()) {
            List<PendingAck> acks = new ArrayList<>(pendingAcks);
            pendingAcks.clear();
            for (PendingAck a : acks) {
                // An ack from an older term, or for entries a newer leader has since truncated, is
                // dropped: the leader retransmits (Raft tolerates lost messages).
                if (a.term() == currentTerm && a.matchIndex() <= durableIndex && role != Role.LEADER) {
                    send(a.to(), new AppendEntriesReply(currentTerm, true, a.matchIndex(), 0, 0, a.round()));
                }
            }
        }
    }

    /** Smallest index of the run of {@code term} entries that ends at or before {@code fromIndex}. */
    private long firstIndexOfTerm(long term, long fromIndex) {
        long i = fromIndex;
        while (i - 1 > log.snapshotIndex() && log.termAt(i - 1) == term) {
            i--;
        }
        return i;
    }

    // ---------------------------------------------------------------- InstallSnapshot (receiver)

    /**
     * Figure 13 receiver implementation, with chunks assembled in memory:
     * <ol>
     *   <li>Reply immediately if term &lt; currentTerm.</li>
     *   <li>Create a new snapshot buffer if first chunk (offset is 0).</li>
     *   <li>Write data into the buffer at the given offset.</li>
     *   <li>Reply and wait for more data chunks if done is false.</li>
     *   <li>Save the snapshot (durably), discard any existing or partial snapshot with a smaller index.</li>
     *   <li>If an existing log entry has the same index and term as the snapshot's last included
     *       entry, retain the log entries following it and reply.</li>
     *   <li>Discard the entire log.</li>
     *   <li>Reset the state machine using the snapshot contents.</li>
     * </ol>
     */
    private void onInstallSnapshot(NodeId from, InstallSnapshot is) {
        if (is.term() < currentTerm) {
            // Rule 1.
            send(from, new InstallSnapshotReply(currentTerm, is.lastIncludedIndex(), 0, false));
            return;
        }
        if (is.term() > currentTerm) {
            return; // only reachable with Bug.LEADER_IGNORES_HIGHER_TERM
        }
        if (!acceptLeader(from)) {
            return;
        }
        long index = is.lastIncludedIndex();
        if (index <= lastApplied) {
            // Our state already includes everything this snapshot holds: never go backwards
            // (SPEC §3.10). Tell the leader we are at least this far.
            incoming = null;
            send(from, new InstallSnapshotReply(currentTerm, index, 0, true));
            return;
        }
        if (is.lastIncludedTerm() < 1) {
            return; // a snapshot covers at least one real entry; ignore nonsense
        }
        IncomingSnapshot in = incoming;
        if (is.offset() == 0 && (in == null || in.index != index || in.term != is.lastIncludedTerm())) {
            // Rule 2. (A late duplicate of the first chunk of the snapshot we are already receiving
            // must not throw away what arrived since: it falls through as out of order below.)
            in = new IncomingSnapshot(index, is.lastIncludedTerm());
            incoming = in;
        }
        if (in == null || in.index != index || in.term != is.lastIncludedTerm() || is.offset() != in.bytes.size()) {
            // Out of order, duplicated, or we lost our partial copy (restart): say where to resume.
            boolean same = in != null && in.index == index && in.term == is.lastIncludedTerm();
            send(from, new InstallSnapshotReply(currentTerm, index, same ? in.bytes.size() : 0, false));
            return;
        }
        byte[] chunk = is.data().toArray();
        in.bytes.write(chunk, 0, chunk.length); // rule 3
        if (!is.done()) {
            send(from, new InstallSnapshotReply(currentTerm, index, in.bytes.size(), false)); // rule 4
            return;
        }
        incoming = null;
        installSnapshot(new Snapshot(index, in.term, Bytes.copyOf(in.bytes.toByteArray())));
        send(from, new InstallSnapshotReply(currentTerm, index, 0, true));
    }

    /** Figure 13 rules 5-8. */
    private void installSnapshot(Snapshot snap) {
        long index = snap.lastIncludedIndex();
        long term = snap.lastIncludedTerm();
        // Rule 5: durable BEFORE the log entries it replaces are discarded. A crash in between
        // leaves the new snapshot plus the old log, which start() reconciles.
        snapshotStore.save(snap);
        latestSnapshot = snap;
        Membership snapMembership = snapshotMembershipOf(snap.data());
        if (log.contains(index) && log.termAt(index) == term) {
            compactLog(index, term, snapMembership); // rule 6: keep what follows
        } else {
            resetLog(index, term, snapMembership);   // rule 7
        }
        restoreFromSnapshotData(snap.data()); // rule 8
        lastApplied = index;
        commitIndex = Math.max(commitIndex, index);
        durableIndex = Math.min(Math.max(durableIndex, index), log.lastIndex());
        snapshotsInstalled++;
        trace("installed snapshot " + index + "@t" + term + " (" + snap.data().length() + " bytes)");
        applyCommitted(); // in case commitIndex was already beyond the snapshot
    }

    // ---------------------------------------------------------------- InstallSnapshot (leader side)

    private void onInstallSnapshotReply(NodeId from, InstallSnapshotReply reply) {
        if (role != Role.LEADER || reply.term() != currentTerm || !leaderState.hasPeer(from)) {
            return;
        }
        leaderState.recordAck(from, 0, clock.nanos());
        LeaderState.SnapshotTransfer t = leaderState.transfer(from);
        if (reply.installed()) {
            // The follower's state now covers the snapshot: everything up to its index matches ours
            // (those entries are committed). Continue with AppendEntries right after it.
            if (reply.lastIncludedIndex() > log.lastIndex()) {
                throw new IllegalStateException(from + " claims a snapshot at " + reply.lastIncludedIndex()
                        + " beyond our last index " + log.lastIndex());
            }
            long matchBefore = leaderState.matchIndex(from);
            leaderState.recordMatch(from, reply.lastIncludedIndex());
            if (t != null && t.snapshot.lastIncludedIndex() <= reply.lastIncludedIndex()) {
                leaderState.endTransfer(from);
                trace("snapshot " + reply.lastIncludedIndex() + " reached " + from);
            }
            advanceCommitIndex();
            if (leaderState.matchIndex(from) > matchBefore) {
                replicateTo(from);
            }
            return;
        }
        if (t == null || t.snapshot.lastIncludedIndex() != reply.lastIncludedIndex()) {
            return; // about a transfer that is over
        }
        long next = Math.min(reply.nextOffset(), t.snapshot.data().length());
        if (next > t.acked) {
            t.acked = next;                        // progress: open the window further
            t.sent = Math.max(t.sent, next);
            sendSnapshotChunks(from, t);
        } else if (next == 0 && t.acked > 0) {
            t.acked = 0;                           // the follower lost its partial copy (restart): start over
            t.sent = 0;
            sendSnapshotChunks(from, t);
        }
        // Anything else is stale (an old or duplicated reply), or a chunk rejected because one
        // before it got lost. The heartbeat goes back to the confirmed offset; reacting to every
        // such reply would multiply messages (each burst of chunks produces a burst of replies).
    }

    /** At most this many chunks are in flight to one follower. */
    static final int SNAPSHOT_WINDOW = 8;

    /** Sends chunks from {@code t.sent} until the window is full or the whole snapshot is in flight. */
    private void sendSnapshotChunks(NodeId peer, LeaderState.SnapshotTransfer t) {
        Snapshot snap = t.snapshot;
        int size = snap.data().length();
        long window = (long) SNAPSHOT_WINDOW * config.snapshotChunkBytes();
        if (size == 0) {
            sendChunk(peer, t, 0, 0, true); // an empty state is one (empty, final) chunk
            return;
        }
        while (t.sent < size && t.sent - t.acked < window) {
            int offset = (int) t.sent;
            int length = Math.min(config.snapshotChunkBytes(), size - offset);
            sendChunk(peer, t, offset, length, offset + length == size);
            t.sent = offset + length;
        }
    }

    private void sendChunk(NodeId peer, LeaderState.SnapshotTransfer t, int offset, int length, boolean done) {
        Snapshot snap = t.snapshot;
        t.lastSentNanos = clock.nanos();
        send(peer, new InstallSnapshot(currentTerm, self, snap.lastIncludedIndex(), snap.lastIncludedTerm(),
                offset, snap.data().slice(offset, length), done));
    }

    // ---------------------------------------------------------------- AppendEntries (leader side)

    private void onAppendEntriesReply(NodeId from, AppendEntriesReply reply) {
        if (role != Role.LEADER || reply.term() != currentTerm || !leaderState.hasPeer(from)) {
            return; // stale reply (a higher term has already been handled in handle()), or a removed server
        }
        // Any reply of our term, success or not, means the follower still accepts us as leader.
        leaderState.recordAck(from, reply.round(), clock.nanos());
        confirmRounds();
        if (reply.success()) {
            if (reply.matchIndex() > log.lastIndex()) {
                throw new IllegalStateException(from + " claims to match index " + reply.matchIndex()
                        + " beyond our last index " + log.lastIndex());
            }
            long matchBefore = leaderState.matchIndex(from);
            leaderState.recordMatch(from, reply.matchIndex());
            advanceCommitIndex();
            if (role != Role.LEADER) {
                return; // committing a configuration that removed us made us step down
            }
            // Flow control: only a reply that made PROGRESS keeps the entries flowing. Replies to
            // older, overlapping or duplicated messages don't trigger sends; otherwise every
            // duplicate would start another never-ending chain of messages (SPEC §3.5).
            if (reply.matchIndex() > matchBefore) {
                leaderState.recordProgress(from, clock.nanos());
                checkPromotion(from);
                checkTransfer();
                if (leaderState.sentUpTo(from) < log.lastIndex()) {
                    replicateTo(from);
                }
            }
            return;
        }
        // Rejected: back up nextIndex using the follower's hints (§5.3 optimization).
        long next;
        if (reply.conflictTerm() != 0) {
            long lastOfTerm = lastIndexOfTerm(reply.conflictTerm());
            next = lastOfTerm > 0 ? lastOfTerm + 1 : reply.conflictIndex();
        } else {
            next = reply.conflictIndex();
        }
        // Never move forward on a rejection (it may be stale), never below what is known to match.
        long current = leaderState.nextIndex(from);
        next = Math.min(next, current);
        next = Math.max(next, leaderState.matchIndex(from) + 1);
        if (next < current) {
            leaderState.setNextIndex(from, next);
            leaderState.resetWindow(from);
            replicateTo(from); // retry at once from the better position
        } else if (reply.conflictTerm() == 0 && leaderState.goBackOnce(from, reply.conflictIndex() - 1)) {
            // Pipelined batches overtook one another (or one got lost): the follower's log ends
            // before batches we already sent. Resend from its end instead of waiting for a heartbeat.
            replicateTo(from);
        }
        // A rejection that teaches us nothing new (stale or duplicated) sends nothing; the next
        // heartbeat retries anyway.
    }

    /** Last index in our log whose term is {@code term}, or 0 if we have none (or can't tell). */
    private long lastIndexOfTerm(long term) {
        for (long i = log.lastIndex(); i > log.snapshotIndex(); i--) {
            long t = log.termAt(i);
            if (t == term) {
                return i;
            }
            if (t < term) {
                return 0; // terms only decrease going backwards
            }
        }
        return 0;
    }

    private void replicateToAll() {
        for (NodeId peer : List.copyOf(leaderState.peers())) {
            replicateTo(peer);
        }
    }

    /**
     * Sends the next batch of entries (possibly none: a heartbeat) to {@code peer}, starting at
     * its nextIndex. Messages may overlap or arrive twice; followers handle that idempotently.
     */
    private void replicateTo(NodeId peer) {
        long next = leaderState.nextIndex(peer);
        if (next <= log.snapshotIndex()) {
            // The entries this follower needs are gone from our log: send the snapshot (§7).
            LeaderState.SnapshotTransfer t = leaderState.transfer(peer);
            if (t == null) {
                t = leaderState.startTransfer(peer, latestSnapshot);
                trace("sending snapshot " + latestSnapshot.lastIncludedIndex() + " to " + peer);
            } else if (clock.nanos() - t.lastSentNanos < config.heartbeatInterval().toNanos()) {
                return; // chunks are in flight; don't add more on every proposal
            } else {
                t.sent = t.acked; // nothing heard for a heartbeat: go back to what is confirmed
            }
            sendSnapshotChunks(peer, t);
            return;
        }
        leaderState.endTransfer(peer);
        // Pipelining (SPEC §3.5): up to maxInflightAppends batches in flight, each continuing where
        // the previous one ended. A window that is full, or nothing new to send, sends nothing,
        // except a heartbeat: when the window is empty we always send (possibly no entries).
        boolean sent = false;
        while (leaderState.inflight(peer) < config.maxInflightAppends()) {
            long from = Math.max(next, leaderState.sentUpTo(peer) + 1);
            if (from > log.lastIndex() && (sent || leaderState.inflight(peer) > 0)) {
                break;
            }
            long prevIndex = from - 1;
            List<LogEntry> entries = from <= log.lastIndex()
                    ? log.entriesFrom(from, config.maxEntriesPerAppend())
                    : List.of();
            send(peer, new AppendEntries(currentTerm, self, prevIndex, log.termAt(prevIndex), entries, commitIndex, round));
            sent = true;
            if (entries.isEmpty()) {
                break; // a heartbeat; not counted in the window
            }
            leaderState.recordSend(peer, prevIndex + entries.size());
        }
    }

    /**
     * Fig.2 Leaders: "If there exists an N such that N > commitIndex, a majority of
     * matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)".
     *
     * <p>The term condition is the fix for Figure 8: an entry from an earlier term may be stored
     * on a majority and still be overwritten by a future leader, so counting replicas is only
     * safe for entries of the leader's own term. Earlier entries then commit indirectly.
     */
    private void advanceCommitIndex() {
        List<Long> matches = new ArrayList<>();
        for (NodeId v : membership.voters()) {
            if (v.equals(self)) {
                // The leader's own copy counts only once it is on disk (thesis §10.2.1).
                matches.add(config.has(Bug.REPLY_BEFORE_FSYNC) ? log.lastIndex() : durableIndex);
            } else {
                matches.add(leaderState.hasPeer(v) ? leaderState.matchIndex(v) : 0L);
            }
        }
        // (A leader that is being removed is not among the voters: it doesn't count itself.)
        matches.sort(Collections.reverseOrder());
        long n = matches.get(membership.quorum() - 1); // highest index stored on a majority
        if (n <= commitIndex) {
            return;
        }
        if (n <= log.snapshotIndex()) {
            return; // can't happen (our snapshot only covers committed entries); be safe anyway
        }
        if (log.termAt(n) != currentTerm && !config.has(Bug.COMMIT_PREVIOUS_TERM_BY_COUNT)) {
            return; // stored on a majority, but not safe to commit by counting (Figure 8)
        }
        commitIndex = n;
        applyCommitted();
    }

    /** Fig.2 All Servers: "If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine". */
    private void applyCommitted() {
        if (lastApplied < commitIndex) {
            trace("commit " + commitIndex);
        }
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.entryAt(lastApplied);
            Bytes result = entry.type() == EntryType.COMMAND
                    ? stateMachine.apply(entry.index(), entry.data())
                    : Bytes.EMPTY;
            if (entry.type() == EntryType.CONFIGURATION) {
                trace("configuration committed at " + entry.index() + ": " + Membership.decode(entry.data()));
            }
            Pending p = pending.remove(entry.index());
            if (p != null) {
                if (p.term() == entry.term()) {
                    p.future().complete(new CommitResult(entry.index(), entry.term(), result));
                } else {
                    // A different leader's entry ended up at this index: ours was lost (SPEC §3.6).
                    p.future().completeExceptionally(new NotLeaderException(leaderId));
                }
            }
        }
        maybeSnapshot();
        serveReads();
        if (role == Role.LEADER && !afterTermCommit.isEmpty() && committedInCurrentTerm() && lastConfigIndex() <= commitIndex) {
            List<Runnable> ready = new ArrayList<>(afterTermCommit);
            afterTermCommit.clear();
            ready.forEach(Runnable::run);
        }
        if (role == Role.LEADER && commitIndex >= lastConfigIndex()) {
            // Removed servers kept receiving entries until their removal committed (so they learn
            // they are out and stop campaigning); now forget them.
            for (NodeId p : List.copyOf(leaderState.peers())) {
                if (!membership.isMember(p)) {
                    leaderState.removePeer(p);
                }
            }
        }
        if (role == Role.LEADER && !membership.isVoter(self) && commitIndex >= lastConfigIndex()) {
            // Thesis §4.2.2: a leader removed from the membership manages the cluster until the
            // change commits, then steps down.
            becomeFollower("removed from the group by the committed configuration");
        }
    }

    /**
     * Takes a snapshot at lastApplied once enough applied entries are not covered by the latest
     * one (SPEC §3.8), saves it durably, and only then discards the log prefix it covers.
     *
     * <p>Serialization happens right here on the event loop (see docs/design.md, Deviations).
     */
    private void maybeSnapshot() {
        long threshold = config.snapshotThreshold();
        if (threshold <= 0 || lastApplied - log.snapshotIndex() < threshold) {
            return;
        }
        long index = lastApplied;
        long term = log.termAt(index);
        Membership atIndex = membershipAt(index);
        Snapshot snap = new Snapshot(index, term, wrapSnapshot(atIndex, stateMachine.snapshot()));
        snapshotStore.save(snap);   // durable first ...
        latestSnapshot = snap;
        compactLog(index, term, atIndex); // ... then the covered entries can go
        durableIndex = Math.min(Math.max(durableIndex, index), log.lastIndex());
        snapshotsTaken++;
        trace("took snapshot " + index + "@t" + term + " (" + snap.data().length() + " bytes)");
    }

    private void failPending() {
        if (pending.isEmpty()) {
            return;
        }
        List<Pending> waiting = new ArrayList<>(pending.values());
        pending.clear();
        for (Pending p : waiting) {
            p.future().completeExceptionally(new NotLeaderException(leaderId));
        }
    }

    // =====================================================================================
    // Role transitions
    // =====================================================================================

    /** Election timer fired. */
    private void onElectionTimeout() {
        if (!running || role == Role.LEADER) {
            return;
        }
        if (!membership.isVoter(self)) {
            // Learners (and servers not yet, or no longer, in the membership) never campaign.
            resetElectionTimer();
            return;
        }
        // Fig.2 Followers: "If election timeout elapses without receiving AppendEntries from
        // current leader or granting vote to candidate: convert to candidate".
        // Fig.2 Candidates: "If election timeout elapses: start new election".
        if (config.preVote()) {
            startPreVote(); // thesis §9.6: first find out whether we could win
        } else {
            startElection(false);
        }
    }

    /**
     * Pre-vote (thesis §9.6): ask the voters whether they would vote for us in term
     * currentTerm + 1, without changing our term. A server that was partitioned away and can't win
     * therefore never raises its term and never forces a healthy leader to step down.
     */
    private void startPreVote() {
        role = Role.PRE_CANDIDATE;
        leaderId = null;
        preVotesGranted.clear();
        preVotesResponded.clear();
        preVotesGranted.add(self);
        preVotesStarted++;
        trace("starting pre-vote for t" + (currentTerm + 1));
        resetElectionTimer(); // no majority within a timeout: try again
        if (countVoters(preVotesGranted) >= membership.quorum()) {
            startElection(false); // single-voter group
            return;
        }
        sendPreVoteRequests();
    }

    private void sendPreVoteRequests() {
        if (!running || role != Role.PRE_CANDIDATE) {
            return;
        }
        RequestVote request = new RequestVote(currentTerm + 1, self, log.lastIndex(), log.lastTerm(), true, false);
        for (NodeId peer : membership.voters()) {
            if (!peer.equals(self) && !preVotesResponded.contains(peer)) {
                send(peer, request);
            }
        }
        cancel(voteRetryTimer);
        long termAtSend = currentTerm;
        voteRetryTimer = scheduler.schedule(config.heartbeatInterval(), () -> {
            if (role == Role.PRE_CANDIDATE && currentTerm == termAtSend) {
                sendPreVoteRequests();
            }
        });
    }

    /**
     * @param transfer started by TimeoutNow (leadership transfer): voters skip the stickiness
     *                 check for this election (thesis §3.10)
     */
    private void startElection(boolean transfer) {
        // Fig.2 Candidates, "On conversion to candidate, start election":
        currentTerm++;          //   - Increment currentTerm
        votedFor = self;        //   - Vote for self
        role = Role.CANDIDATE;
        leaderId = null;
        heardFromLeader = false;
        transferElection = transfer;
        persist();
        votesGranted.clear();
        votesResponded.clear();
        votesGranted.add(self);
        electionsStarted++;
        trace("starting election" + (transfer ? " (leadership transfer)" : ""));
        resetElectionTimer();   //   - Reset election timer
        if (countVoters(votesGranted) >= membership.quorum()) {
            becomeLeader();     // single-voter group: our own vote is a majority
            return;
        }
        sendVoteRequests();     //   - Send RequestVote RPCs to all other servers
    }

    /**
     * Sends RequestVote to every voter that has not answered yet, and keeps re-sending every
     * heartbeat interval while the election lasts. The paper (§5.1): "Servers retry RPCs if
     * they do not receive a response in a timely manner". Without retries, one lost message
     * can waste a whole election timeout on a lossy network.
     */
    private void sendVoteRequests() {
        if (!running || role != Role.CANDIDATE) {
            return;
        }
        RequestVote request = new RequestVote(currentTerm, self, log.lastIndex(), log.lastTerm(), false, transferElection);
        for (NodeId peer : membership.voters()) {
            if (!peer.equals(self) && !votesResponded.contains(peer)) {
                send(peer, request);
            }
        }
        cancel(voteRetryTimer);
        long termAtSend = currentTerm;
        voteRetryTimer = scheduler.schedule(config.heartbeatInterval(), () -> {
            if (role == Role.CANDIDATE && currentTerm == termAtSend) {
                sendVoteRequests();
            }
        });
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = self;
        cancel(electionTimer);
        cancel(voteRetryTimer);
        trace("became leader with votes from " + votesGranted);
        // Fig.2: nextIndex starts at leader last log index + 1, matchIndex at 0.
        long now = clock.nanos();
        List<NodeId> peers = membership.members().stream().filter(p -> !p.equals(self)).toList();
        leaderState = new LeaderState(peers, log.lastIndex(), now);
        round = 0;
        confirmedRound = 0;
        roundSentNanos.clear();
        leaseExpiryNanos = Long.MIN_VALUE;
        // §8: "each leader commit a blank no-op entry into the log at the start of its term".
        // Entries from earlier terms can only commit indirectly, through an entry of the
        // current term (Figure 8), so without this they could wait forever for a client write.
        // ReadIndex needs it too: reads wait until an entry of the leader's term has committed.
        if (!config.has(Bug.NO_NOOP_ON_ELECTION)) {
            appendToLog(List.of(LogEntry.noop(log.lastIndex() + 1, currentTerm)));
            scheduleFlush();
        }
        if (config.checkQuorum()) {
            scheduleCheckQuorum();
        }
        // Fig.2 Leaders: "Upon election: send initial empty AppendEntries RPCs (heartbeat) to
        // each server; repeat during idle periods to prevent election timeouts (§5.2)".
        sendHeartbeats();
        advanceCommitIndex(); // a single-voter group commits on its own (once the flush ran)
    }

    /**
     * Every heartbeat interval: a new heartbeat round to every member. Each round's number travels
     * in the AppendEntries and comes back in the replies, which is how ReadIndex and leases learn
     * that a majority still followed us at a known time.
     */
    private void sendHeartbeats() {
        if (!running || role != Role.LEADER) {
            return;
        }
        broadcastRound();
        checkTransfer();
        cancel(heartbeatTimer);
        heartbeatTimer = scheduler.schedule(config.heartbeatInterval(), this::sendHeartbeats);
    }

    /** Starts heartbeat round {@code round + 1} and sends it to every member. */
    private void broadcastRound() {
        round++;
        roundSentNanos.put(round, clock.nanos());
        long now = clock.nanos();
        for (NodeId peer : List.copyOf(leaderState.peers())) {
            if (leaderState.inflight(peer) > 0
                    && now - leaderState.lastProgressNanos(peer) > config.heartbeatInterval().toNanos()) {
                leaderState.resetWindow(peer); // nothing confirmed for a while: go back to nextIndex (go-back-N)
            }
            if (leaderState.inflight(peer) == 0 || leaderState.transfer(peer) != null) {
                replicateTo(peer); // entries the follower is missing, or an empty heartbeat
            } else {
                // Batches are in flight: a bare heartbeat carries the round and commit index.
                long prev = leaderState.nextIndex(peer) - 1;
                if (prev < log.snapshotIndex()) {
                    replicateTo(peer);
                } else {
                    send(peer, new AppendEntries(currentTerm, self, prev, log.termAt(prev), List.of(), commitIndex, round));
                }
            }
        }
        confirmRounds(); // a single-voter group confirms on its own
    }

    /** Adopts a higher term and becomes a follower (Fig.2 All Servers). */
    private void stepDown(long newTerm, String why) {
        currentTerm = newTerm;
        votedFor = null;
        leaderId = null;
        heardFromLeader = false;
        persist();
        if (role == Role.LEADER || role == Role.CANDIDATE || role == Role.PRE_CANDIDATE) {
            becomeFollower(why);
        } else {
            trace("adopted t" + newTerm + ": " + why);
        }
    }

    private void becomeFollower(String why) {
        Role previous = role;
        role = membership.isVoter(self) ? Role.FOLLOWER : Role.LEARNER;
        leaderState = null;
        failPending();
        failLeaderWork(new NotLeaderException(leaderId));
        cancel(heartbeatTimer);
        cancel(voteRetryTimer);
        cancel(checkQuorumTimer);
        votesGranted.clear();
        votesResponded.clear();
        preVotesGranted.clear();
        preVotesResponded.clear();
        resetElectionTimer();
        trace("stepped down from " + previous + ": " + why);
    }

    /** Fails everything only a leader does: reads, promotions, a transfer. */
    private void failLeaderWork(RuntimeException why) {
        leaseExpiryNanos = Long.MIN_VALUE;
        List<PendingRead> reads = new ArrayList<>(pendingReads);
        pendingReads.clear();
        for (PendingRead r : reads) {
            r.future.completeExceptionally(why);
        }
        List<Runnable> deferred = new ArrayList<>(afterTermCommit);
        afterTermCommit.clear();
        deferred.forEach(Runnable::run); // no longer leader: each fails with NotLeaderException
        List<CompletableFuture<Void>> promotions = new ArrayList<>(promoteWhenCaughtUp.values());
        promoteWhenCaughtUp.clear();
        for (CompletableFuture<Void> f : promotions) {
            f.completeExceptionally(why);
        }
        if (transferFuture != null) {
            CompletableFuture<Void> f = transferFuture;
            // We sent TimeoutNow and then lost leadership: the target started its election.
            boolean succeeded = timeoutNowSent;
            transferTarget = null;
            transferFuture = null;
            if (succeeded) {
                f.complete(null);
            } else {
                f.completeExceptionally(why);
            }
        }
    }

    // =====================================================================================
    // Check-quorum (SPEC §3.4)
    // =====================================================================================

    private void scheduleCheckQuorum() {
        cancel(checkQuorumTimer);
        checkQuorumTimer = scheduler.schedule(config.electionTimeoutMin(), this::checkQuorum);
    }

    /**
     * A leader that hasn't heard from a majority of voters (counting itself) within one minimum
     * election timeout steps down: it is probably partitioned away, and the others may already
     * have elected a new leader. Leases rely on this.
     */
    private void checkQuorum() {
        if (!running || role != Role.LEADER) {
            return;
        }
        long now = clock.nanos();
        long window = config.electionTimeoutMin().toNanos();
        int heard = 0;
        for (NodeId v : membership.voters()) {
            if (v.equals(self) || (leaderState.hasPeer(v) && now - leaderState.lastAckNanos(v) < window)) {
                heard++;
            }
        }
        if (heard < membership.quorum()) {
            becomeFollower("check-quorum: heard from only " + heard + " of " + membership.voters().size() + " voters");
            return;
        }
        scheduleCheckQuorum();
    }

    // =====================================================================================
    // Linearizable reads (SPEC §4.4)
    // =====================================================================================

    /**
     * Linearizable read without a log entry. Completes with an index once (a) this node is
     * confirmed to still be leader and (b) it has applied everything up to that index; the caller
     * then reads its local state machine (on this event loop, right away). Fails with
     * {@link NotLeaderException} if this node is not, or stops being, the leader.
     *
     * <ul>
     *   <li>{@link ReadMode#READ_INDEX}: readIndex = commitIndex (once an entry of our term has
     *       committed), confirmed by a heartbeat round started after the read arrived and
     *       acknowledged by a majority. Concurrent reads share one round.</li>
     *   <li>{@link ReadMode#LEASE}: no round trip while the lease from an earlier confirmed round is
     *       valid; otherwise like READ_INDEX.</li>
     * </ul>
     */
    public CompletableFuture<Long> read(ReadMode mode) {
        if (mode == ReadMode.LOG) {
            throw new IllegalArgumentException("LOG reads go through propose()");
        }
        if (!running || role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(running ? leaderId : null));
        }
        if (transferTarget != null) {
            return CompletableFuture.failedFuture(new NotLeaderException(transferTarget));
        }
        CompletableFuture<Long> future = new CompletableFuture<>();
        pendingReads.add(new PendingRead(future, mode));
        serveReads();
        return future;
    }

    /** Moves every pending read as far as it can go. */
    private void serveReads() {
        if (pendingReads.isEmpty() || role != Role.LEADER) {
            return;
        }
        boolean termCommitted = commitIndex > 0 && commitIndex > log.snapshotIndex()
                ? log.termAt(commitIndex) == currentTerm
                : commitIndex > 0 && log.snapshotTerm() == currentTerm;
        boolean needRound = false;
        long now = clock.nanos();
        java.util.Iterator<PendingRead> it = pendingReads.iterator();
        while (it.hasNext()) {
            PendingRead r = it.next();
            if (r.readIndex < 0) {
                if (!termCommitted) {
                    continue; // thesis §6.4 step 1: wait until an entry of our term has committed
                }
                r.readIndex = commitIndex;
                if (r.mode == ReadMode.LEASE && now < leaseExpiryNanos) {
                    r.confirmed = true;                         // inside the lease: no round trip
                } else if (config.has(Bug.READINDEX_WITHOUT_HEARTBEAT)) {
                    r.confirmed = true;                         // BUG: skip the leadership check
                } else {
                    r.round = round + 1;                        // the next round confirms it
                    needRound = true;
                }
            }
            if (!r.confirmed && r.round > 0 && r.round <= confirmedRound) {
                r.confirmed = true;
            }
            if (r.confirmed && lastApplied >= r.readIndex) {
                it.remove();
                r.future.complete(r.readIndex);
            }
        }
        if (needRound && !roundScheduled) {
            // Every read that arrives before this runs shares the same round (batching).
            roundScheduled = true;
            scheduler.execute(() -> {
                roundScheduled = false;
                if (running && role == Role.LEADER) {
                    broadcastRound();
                }
            });
        }
    }

    /**
     * Finds the newest heartbeat round a majority of voters has echoed. Its send time starts a
     * lease (thesis §6.4.1): no other leader can be elected until the followers that acknowledged
     * it have gone a minimum election timeout without hearing from us.
     */
    private void confirmRounds() {
        if (role != Role.LEADER) {
            return;
        }
        List<Long> acked = new ArrayList<>();
        for (NodeId v : membership.voters()) {
            if (v.equals(self)) {
                acked.add(round);
            } else {
                acked.add(leaderState.hasPeer(v) ? leaderState.ackedRound(v) : 0L);
            }
        }
        acked.sort(Collections.reverseOrder());
        long confirmed = acked.get(membership.quorum() - 1);
        if (confirmed > confirmedRound) {
            confirmedRound = confirmed;
            Long sentAt = roundSentNanos.get(confirmed);
            if (sentAt != null && transferTarget == null) {
                leaseExpiryNanos = Math.max(leaseExpiryNanos, sentAt + config.leaseDuration().toNanos());
            }
            roundSentNanos.headMap(confirmed, false).clear();
            serveReads();
        }
    }

    // =====================================================================================
    // Membership changes (thesis ch. 4)
    // =====================================================================================

    /**
     * Proposes {@code next} as the group's membership. It takes effect on each server as soon as
     * the entry is appended there; the future completes when it is committed. Fails with
     * {@link NotLeaderException} on a non-leader and {@link IllegalStateException} unless {@code next}
     * differs from the current membership by one server. While a previous change is uncommitted, or
     * no entry of this leader's term has committed yet (the safety fix of Ongaro, raft-dev 2015),
     * the change waits instead of being appended.
     */
    public CompletableFuture<CommitResult> changeMembership(Membership next) {
        if (!running || role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(running ? leaderId : null));
        }
        if (transferTarget != null) {
            return CompletableFuture.failedFuture(new NotLeaderException(transferTarget));
        }
        if (!membership.isSingleServerChangeTo(next)) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "only single-server changes are allowed: " + membership + " -> " + next));
        }
        boolean previousPending = lastConfigIndex() > commitIndex;
        boolean termUncommitted = !committedInCurrentTerm() && !config.has(Bug.CONFIG_CHANGE_BEFORE_CURRENT_TERM_COMMIT);
        if (previousPending || termUncommitted) {
            // One change at a time, and the safety rule: wait (don't append) until the previous
            // change and an entry of our own term (our no-op, at least) have committed.
            CompletableFuture<CommitResult> later = new CompletableFuture<>();
            afterTermCommit.add(() -> changeMembership(next).whenComplete((r, e) -> {
                if (e != null) {
                    later.completeExceptionally(e);
                } else {
                    later.complete(r);
                }
            }));
            trace("membership change to " + next + " waits for " + (previousPending
                    ? "the previous change (at " + lastConfigIndex() + ") to commit" : "an entry of t" + currentTerm + " to commit"));
            return later;
        }
        trace("proposing membership " + next);
        return appendAsLeader(EntryType.CONFIGURATION, next.encode());
    }

    /**
     * Adds {@code id} as a voter the safe way: first as a learner, which receives the log (or a
     * snapshot), then promoted once it has caught up with the commit index. Completes when the
     * promotion has committed; already a voter completes at once. Retry on failure: every step is
     * idempotent.
     */
    public CompletableFuture<Void> addServer(NodeId id) {
        if (!running || role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(running ? leaderId : null));
        }
        if (membership.isVoter(id)) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> done = promoteWhenCaughtUp.computeIfAbsent(id, k -> new CompletableFuture<>());
        if (!membership.isLearner(id)) {
            changeMembership(membership.withLearner(id)).whenComplete((r, e) -> {
                if (e != null) {
                    promoteWhenCaughtUp.remove(id);
                    done.completeExceptionally(e);
                }
            });
        } else {
            checkPromotion(id);
        }
        return done;
    }

    /** Removes a voter or learner. Removing the leader itself makes it step down once committed. */
    public CompletableFuture<CommitResult> removeServer(NodeId id) {
        if (!membership.isMember(id)) {
            return CompletableFuture.failedFuture(new IllegalStateException(id + " is not a member"));
        }
        if (membership.isVoter(id) && membership.voters().size() == 1) {
            return CompletableFuture.failedFuture(new IllegalStateException("can't remove the last voter"));
        }
        return changeMembership(membership.without(id));
    }

    /** A learner we were asked to promote has made progress: promote it once it has caught up. */
    private void checkPromotion(NodeId id) {
        CompletableFuture<Void> done = promoteWhenCaughtUp.get(id);
        if (done == null || role != Role.LEADER) {
            return;
        }
        if (membership.isVoter(id)) {
            promoteWhenCaughtUp.remove(id);
            done.complete(null);
            return;
        }
        if (!membership.isLearner(id) || !leaderState.hasPeer(id)) {
            return;
        }
        boolean caughtUp = leaderState.matchIndex(id) >= commitIndex;
        if (!caughtUp || lastConfigIndex() > commitIndex || !committedInCurrentTerm()) {
            return;
        }
        changeMembership(membership.promoted(id)).whenComplete((r, e) -> {
            promoteWhenCaughtUp.remove(id);
            if (e != null) {
                done.completeExceptionally(e);
            } else {
                done.complete(null);
            }
        });
    }

    private boolean committedInCurrentTerm() {
        if (commitIndex == 0) {
            return false;
        }
        long t = commitIndex == log.snapshotIndex() ? log.snapshotTerm() : log.termAt(commitIndex);
        return t == currentTerm;
    }

    // =====================================================================================
    // Leadership transfer (thesis §3.10)
    // =====================================================================================

    /**
     * Hands leadership to {@code target}: stop accepting proposals, bring the target's log up to
     * date, then send it TimeoutNow so it starts an election at once (its voters skip the
     * stickiness check). Completes when the target has become leader; fails after one election
     * timeout without success, and the leader resumes normal work.
     */
    public CompletableFuture<Void> transferLeadership(NodeId target) {
        if (!running || role != Role.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(running ? leaderId : null));
        }
        if (target.equals(self)) {
            return CompletableFuture.completedFuture(null);
        }
        if (!membership.isVoter(target)) {
            return CompletableFuture.failedFuture(new IllegalStateException(target + " is not a voter"));
        }
        if (transferTarget != null) {
            return CompletableFuture.failedFuture(new IllegalStateException("already transferring to " + transferTarget));
        }
        transferTarget = target;
        timeoutNowSent = false;
        transferDeadlineNanos = clock.nanos() + config.electionTimeoutMin().toNanos();
        transferFuture = new CompletableFuture<>();
        leaseExpiryNanos = Long.MIN_VALUE; // the target's election skips stickiness: no more lease reads
        trace("transferring leadership to " + target);
        CompletableFuture<Void> result = transferFuture;
        replicateTo(target);
        checkTransfer();
        return result;
    }

    /** Sends TimeoutNow once the target has everything; aborts after an election timeout. */
    private void checkTransfer() {
        if (transferTarget == null || role != Role.LEADER) {
            return;
        }
        if (clock.nanos() > transferDeadlineNanos) {
            trace("leadership transfer to " + transferTarget + " timed out");
            CompletableFuture<Void> f = transferFuture;
            NodeId target = transferTarget;
            transferTarget = null;
            transferFuture = null;
            f.completeExceptionally(new java.util.concurrent.TimeoutException("transfer to " + target + " timed out"));
            return;
        }
        if (leaderState.hasPeer(transferTarget) && leaderState.matchIndex(transferTarget) >= log.lastIndex()) {
            send(transferTarget, new TimeoutNow(currentTerm, self));
            timeoutNowSent = true;
        }
    }

    /** The leader asks us to take over: campaign now, skipping pre-vote and stickiness. */
    private void onTimeoutNow(NodeId from, TimeoutNow m) {
        if (m.term() != currentTerm || !from.equals(leaderId) || !membership.isVoter(self)
                || role == Role.LEADER || role == Role.CANDIDATE) {
            return;
        }
        trace("TimeoutNow from " + from);
        startElection(true);
    }

    // =====================================================================================
    // Log changes that keep the membership in step (a configuration takes effect when appended)
    // =====================================================================================

    private void appendToLog(List<LogEntry> entries) {
        log.append(entries);
        boolean changed = false;
        for (LogEntry e : entries) {
            if (e.type() == EntryType.CONFIGURATION) {
                configEntries.put(e.index(), Membership.decode(e.data()));
                changed = true;
            }
        }
        if (changed) {
            refreshMembership();
        }
    }

    private void truncateLog(long fromIndex) {
        log.truncateSuffix(fromIndex);
        pendingAcks.removeIf(a -> a.matchIndex() >= fromIndex);
        if (!configEntries.tailMap(fromIndex, true).isEmpty()) {
            configEntries.tailMap(fromIndex, true).clear();
            refreshMembership(); // a truncated configuration reverts to the previous one
        }
    }

    private void compactLog(long index, long term, Membership atIndex) {
        log.compactPrefix(index, term);
        snapshotMembership = atIndex;
        configEntries.headMap(index, true).clear();
        refreshMembership();
    }

    private void resetLog(long index, long term, Membership atIndex) {
        log.reset(index, term);
        snapshotMembership = atIndex;
        configEntries.clear();
        refreshMembership();
    }

    private Membership currentMembership() {
        if (!configEntries.isEmpty()) {
            return configEntries.lastEntry().getValue();
        }
        return snapshotMembership != null ? snapshotMembership : bootstrapMembership;
    }

    /** The membership in effect at {@code index}: the latest configuration at or before it. */
    private Membership membershipAt(long index) {
        Map.Entry<Long, Membership> e = configEntries.floorEntry(index);
        if (e != null) {
            return e.getValue();
        }
        return snapshotMembership != null ? snapshotMembership : bootstrapMembership;
    }

    /** Index of the newest configuration entry in the log, or the snapshot index if there is none. */
    private long lastConfigIndex() {
        return configEntries.isEmpty() ? log.snapshotIndex() : configEntries.lastKey();
    }

    private void refreshMembership() {
        Membership next = currentMembership();
        if (next.equals(membership)) {
            return;
        }
        Membership previous = membership;
        membership = next;
        trace("membership now " + next + " (was " + previous + ")");
        if (role == Role.LEADER) {
            long now = clock.nanos();
            for (NodeId p : next.members()) {
                if (!p.equals(self) && !leaderState.hasPeer(p)) {
                    leaderState.addPeer(p, log.lastIndex(), now);
                }
            }
            // Peers that left keep getting entries until the change commits (see applyCommitted).
        } else if (role == Role.FOLLOWER || role == Role.LEARNER) {
            role = next.isVoter(self) ? Role.FOLLOWER : Role.LEARNER;
        } else if (!next.isVoter(self)) {
            becomeFollower("no longer a voter"); // a (pre-)candidate that was removed
        }
    }

    // ---- Snapshots carry the membership in effect at their index, in front of the state machine's bytes.

    private static final int SNAPSHOT_MAGIC = 0x524B4D31; // "RKM1"

    private static Bytes wrapSnapshot(Membership m, Bytes state) {
        dev.raftkv.codec.BinaryWriter w = new dev.raftkv.codec.BinaryWriter();
        w.writeInt(SNAPSHOT_MAGIC);
        m.writeTo(w);
        w.writeBytes(state);
        return w.toBytes();
    }

    private static Membership snapshotMembershipOf(Bytes data) {
        dev.raftkv.codec.BinaryReader r = new dev.raftkv.codec.BinaryReader(data);
        if (r.readInt() != SNAPSHOT_MAGIC) {
            throw new IllegalStateException("snapshot in an unknown format (from an older version? start with an empty data directory)");
        }
        return Membership.readFrom(r);
    }

    private void restoreFromSnapshotData(Bytes data) {
        dev.raftkv.codec.BinaryReader r = new dev.raftkv.codec.BinaryReader(data);
        if (r.readInt() != SNAPSHOT_MAGIC) {
            throw new IllegalStateException("snapshot in an unknown format (from an older version? start with an empty data directory)");
        }
        Membership m = Membership.readFrom(r);
        Bytes state = r.readBytes();
        r.expectEnd();
        if (configEntries.isEmpty() || snapshotMembership == null) {
            snapshotMembership = m;
        }
        stateMachine.restore(state);
        membership = currentMembership();
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    private boolean believesLeaderIsAlive() {
        if (role == Role.LEADER) {
            return true;
        }
        if (config.has(Bug.LEASE_WITHOUT_STICKINESS)) {
            return false; // BUG: followers vote while they still hear from a leader
        }
        return heardFromLeader && clock.nanos() - lastLeaderContactNanos < config.electionTimeoutMin().toNanos();
    }

    private int countVoters(Set<NodeId> ids) {
        int n = 0;
        for (NodeId id : ids) {
            if (membership.isVoter(id)) {
                n++;
            }
        }
        return n;
    }

    /** Restarts the election timer with a fresh random timeout in [min, max] (§5.2: "randomized election timeouts"). */
    private void resetElectionTimer() {
        cancel(electionTimer);
        long min = config.electionTimeoutMin().toNanos();
        long max = config.electionTimeoutMax().toNanos();
        long timeout = min + random.nextLong(max - min + 1);
        electionTimer = scheduler.schedule(Duration.ofNanos(timeout), this::onElectionTimeout);
    }

    private void persist() {
        NodeId vote = config.has(Bug.VOTEDFOR_NOT_PERSISTED) ? null : votedFor;
        metaStore.save(new PersistentState(currentTerm, vote));
    }

    private void send(NodeId to, Message message) {
        transport.send(to, group, message);
    }

    private static void cancel(Cancellable timer) {
        if (timer != null) {
            timer.cancel();
        }
    }

    private void trace(String event) {
        tracer.trace("[" + self + " " + group + " t" + currentTerm + " " + role + "] " + event);
    }

    // =====================================================================================
    // Read-only view (tests, metrics, CLI status)
    // =====================================================================================

    public NodeId id() {
        return self;
    }

    public GroupId group() {
        return group;
    }

    /** The voters of the membership currently in effect on this node. */
    public Set<NodeId> voters() {
        return membership.voters();
    }

    /** The membership currently in effect on this node (latest configuration in its log). */
    public Membership membership() {
        return membership;
    }

    public long preVotesStarted() {
        return preVotesStarted;
    }

    /** True while this leader holds a read lease (tests, metrics). */
    public boolean holdsLease() {
        return role == Role.LEADER && clock.nanos() < leaseExpiryNanos;
    }

    public int pendingReads() {
        return pendingReads.size();
    }

    public RaftConfig config() {
        return config;
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public Optional<NodeId> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    /** The leader of the current term, if known (ourselves when we are leader). */
    public Optional<NodeId> leaderId() {
        return Optional.ofNullable(leaderId);
    }

    public boolean isRunning() {
        return running;
    }

    public long electionsStarted() {
        return electionsStarted;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    /** The leader's matchIndex for {@code peer}; empty unless this node is leader. */
    public Optional<Long> matchIndexOf(NodeId peer) {
        return leaderState == null || peer.equals(self) ? Optional.empty() : Optional.of(leaderState.matchIndex(peer));
    }

    /** How many times this node has fsynced its log (tests check group commit with it). */
    public long logSyncs() {
        return logSyncs;
    }

    /** Snapshots this node took itself (tests and metrics). */
    public long snapshotsTaken() {
        return snapshotsTaken;
    }

    /** Snapshots this node installed from a leader (tests and metrics). */
    public long snapshotsInstalled() {
        return snapshotsInstalled;
    }

    /** The snapshot the log is compacted to, if any. */
    public Optional<Snapshot> latestSnapshot() {
        return Optional.ofNullable(latestSnapshot);
    }

    /** Number of proposals still waiting to be applied (tests and metrics). */
    public int pendingProposals() {
        return pending.size();
    }

    public RaftLog log() {
        return log;
    }

    @Override
    public String toString() {
        return self + "(" + role + ", t" + currentTerm + ", leader=" + leaderId
                + ", last=" + log.lastIndex() + ", commit=" + commitIndex + ")";
    }

    // =====================================================================================
    // Builder
    // =====================================================================================

    /** Assembles a node; every dependency is explicit so tests can substitute any of them. */
    public static final class Builder {
        private NodeId self;
        private GroupId group = GroupId.of(1);
        private Set<NodeId> voters;
        private RaftConfig config = RaftConfig.defaults();
        private Scheduler scheduler;
        private Clock clock;
        private Random random;
        private Transport transport;
        private RaftLog log;
        private MetaStore metaStore;
        private StateMachine stateMachine;
        private SnapshotStore snapshotStore;
        private Tracer tracer;

        private Builder() {
        }

        public Builder self(NodeId v) { this.self = v; return this; }
        public Builder group(GroupId v) { this.group = v; return this; }
        public Builder voters(Set<NodeId> v) { this.voters = v; return this; }
        public Builder config(RaftConfig v) { this.config = v; return this; }
        public Builder scheduler(Scheduler v) { this.scheduler = v; return this; }
        public Builder clock(Clock v) { this.clock = v; return this; }
        public Builder random(Random v) { this.random = v; return this; }
        public Builder transport(Transport v) { this.transport = v; return this; }
        public Builder log(RaftLog v) { this.log = v; return this; }
        public Builder metaStore(MetaStore v) { this.metaStore = v; return this; }
        public Builder stateMachine(StateMachine v) { this.stateMachine = v; return this; }
        public Builder snapshotStore(SnapshotStore v) { this.snapshotStore = v; return this; }
        public Builder tracer(Tracer v) { this.tracer = v; return this; }

        public RaftNode build() {
            return new RaftNode(this);
        }
    }
}
