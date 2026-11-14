package dev.raftkv.raft;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
import dev.raftkv.protocol.RequestVote;
import dev.raftkv.protocol.RequestVoteReply;
import dev.raftkv.protocol.TimeoutNow;
import dev.raftkv.raft.log.RaftLog;
import dev.raftkv.raft.storage.MetaStore;
import dev.raftkv.raft.storage.PersistentState;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * One server's replica of one Raft group.
 *
 * <p><b>Milestone 2 scope: leader election.</b> Terms, RequestVote, randomized election
 * timeouts, heartbeats, and leader stickiness. Log replication (AppendEntries with entries,
 * commit index, applying) arrives in Milestone 3; the heartbeat handler already performs the
 * Figure 2 consistency check so that step is a small extension.
 *
 * <p><b>Threading.</b> Every public method must be called on this node's {@link Scheduler}
 * (its event loop). The node never blocks and never creates threads, which is what makes it
 * deterministic under simulation (SPEC §3.2).
 *
 * <p><b>Durability.</b> {@code currentTerm} and {@code votedFor} are saved through the
 * {@link MetaStore} before any message that depends on them is sent (Figure 2: "updated on
 * stable storage before responding to RPCs"). In this milestone {@code save} is synchronous.
 *
 * <p>Comments of the form {@code // Fig.2 ...} point at the exact rule of Figure 2 of the
 * extended Raft paper that the line implements.
 */
public final class RaftNode {

    private final NodeId self;
    private final GroupId group;
    private final SortedSet<NodeId> voters;
    private final RaftConfig config;
    private final Scheduler scheduler;
    private final Clock clock;
    private final Random random;
    private final Transport transport;
    private final RaftLog log;
    private final MetaStore metaStore;
    private final Tracer tracer;

    // ---- Persistent state on all servers (Figure 2); the log is the third piece.
    private long currentTerm;
    private NodeId votedFor;

    // ---- Volatile state.
    private Role role = Role.FOLLOWER;
    private NodeId leaderId;
    private boolean running;

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

    // ---- Counters for tests and metrics.
    private long electionsStarted;

    private RaftNode(Builder b) {
        this.self = Objects.requireNonNull(b.self, "self");
        this.group = Objects.requireNonNull(b.group, "group");
        this.voters = Collections.unmodifiableSortedSet(new TreeSet<>(Objects.requireNonNull(b.voters, "voters")));
        this.config = Objects.requireNonNull(b.config, "config");
        this.scheduler = Objects.requireNonNull(b.scheduler, "scheduler");
        this.clock = Objects.requireNonNull(b.clock, "clock");
        this.random = Objects.requireNonNull(b.random, "random");
        this.transport = Objects.requireNonNull(b.transport, "transport");
        this.log = Objects.requireNonNull(b.log, "log");
        this.metaStore = Objects.requireNonNull(b.metaStore, "metaStore");
        this.tracer = b.tracer == null ? Tracer.NOOP : b.tracer;
        if (!voters.contains(self)) {
            throw new IllegalArgumentException(self + " must be one of the voters " + voters);
        }
        if (!transport.self().equals(self)) {
            throw new IllegalArgumentException("transport belongs to " + transport.self() + ", not " + self);
        }
    }

    public static Builder builder() {
        return new Builder();
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
        if (currentTerm < log.lastTerm()) {
            throw new IllegalStateException("corrupt state: currentTerm " + currentTerm
                    + " is older than the last log term " + log.lastTerm());
        }
        running = true;
        role = Role.FOLLOWER;
        trace("started (votedFor=" + votedFor + ", last log " + log.lastIndex() + "@t" + log.lastTerm() + ")");
        resetElectionTimer();
    }

    /** Stops all activity; volatile state is lost, like a crash. Persistent state stays in the stores. */
    public void stop() {
        running = false;
        cancel(electionTimer);
        cancel(heartbeatTimer);
        cancel(voteRetryTimer);
        trace("stopped");
    }

    // =====================================================================================
    // Incoming messages
    // =====================================================================================

    /** Handles one message from a peer. Must be called on this node's event loop. */
    public void handle(Envelope envelope) {
        if (!running) {
            return;
        }
        NodeId from = envelope.from();
        if (!envelope.group().equals(group) || !voters.contains(from) || from.equals(self)) {
            trace("ignored message from non-member " + from + " for " + envelope.group());
            return;
        }
        Message message = envelope.message();

        // Thesis §4.2.3 leader stickiness: if we believe a leader is alive (we are it, or we
        // heard from it within the minimum election timeout), a RequestVote is a disruption.
        // Reject it WITHOUT adopting its term, so a node that was cut off and comes back with
        // a high term cannot depose a healthy leader. Leadership transfer bypasses this.
        if (message instanceof RequestVote rv && !rv.leadershipTransfer() && believesLeaderIsAlive()) {
            trace("rejected vote for " + from + " (t" + rv.term() + "): current leader is alive");
            send(from, new RequestVoteReply(currentTerm, false, false));
            return;
        }

        // Fig.2 All Servers: "If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)".
        if (message.term() > currentTerm && !(role == Role.LEADER && config.has(Bug.LEADER_IGNORES_HIGHER_TERM))) {
            stepDown(message.term(), "saw higher term from " + from);
        }

        switch (message) {
            case RequestVote m -> onRequestVote(from, m);
            case RequestVoteReply m -> onRequestVoteReply(from, m);
            case AppendEntries m -> onAppendEntries(from, m);
            case AppendEntriesReply m -> onAppendEntriesReply(from, m);
            case InstallSnapshot m -> trace("ignored InstallSnapshot (Milestone 8)");
            case InstallSnapshotReply m -> trace("ignored InstallSnapshotReply (Milestone 8)");
            case TimeoutNow m -> trace("ignored TimeoutNow (Milestone 12)");
        }
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
        if (role != Role.CANDIDATE || reply.term() != currentTerm || reply.preVote()) {
            return;
        }
        votesResponded.add(from);
        if (reply.voteGranted()) {
            votesGranted.add(from);
            trace("got vote from " + from + " (" + votesGranted.size() + "/" + voters.size() + ")");
            // Fig.2 Candidates: "If votes received from majority of servers: become leader".
            if (isMajority(votesGranted.size())) {
                becomeLeader();
            }
        }
    }

    // ---------------------------------------------------------------- AppendEntries (receiver)

    private void onAppendEntries(NodeId from, AppendEntries ae) {
        if (ae.term() < currentTerm) {
            // Fig.2 AppendEntries rule 1: "Reply false if term < currentTerm (§5.1)". Our term
            // in the reply tells the stale leader to step down.
            send(from, new AppendEntriesReply(currentTerm, false, 0, 0, 0));
            return;
        }
        if (ae.term() > currentTerm) {
            return; // only reachable with Bug.LEADER_IGNORES_HIGHER_TERM
        }
        if (role == Role.LEADER) {
            // Election Safety (§5.2): at most one leader per term. Seeing another one is a bug.
            throw new IllegalStateException(self + " is leader of term " + currentTerm
                    + " but received AppendEntries from " + from + " for the same term");
        }
        if (role == Role.CANDIDATE) {
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

        // Fig.2 AppendEntries rule 2: "Reply false if log doesn't contain an entry at
        // prevLogIndex whose term matches prevLogTerm (§5.3)". Appending entries (rules 3-5)
        // is Milestone 3.
        boolean matches = log.contains(ae.prevLogIndex()) && log.termAt(ae.prevLogIndex()) == ae.prevLogTerm();
        send(from, new AppendEntriesReply(currentTerm, matches, matches ? ae.prevLogIndex() : 0, 0, 0));
    }

    private void onAppendEntriesReply(NodeId from, AppendEntriesReply reply) {
        // Milestone 3 tracks replication progress here. For elections, only the term matters,
        // and the generic higher-term rule in handle() has already applied it.
    }

    // =====================================================================================
    // Role transitions
    // =====================================================================================

    /** Election timer fired. */
    private void onElectionTimeout() {
        if (!running || role == Role.LEADER) {
            return;
        }
        // Fig.2 Followers: "If election timeout elapses without receiving AppendEntries from
        // current leader or granting vote to candidate: convert to candidate".
        // Fig.2 Candidates: "If election timeout elapses: start new election".
        startElection();
    }

    private void startElection() {
        // Fig.2 Candidates, "On conversion to candidate, start election":
        currentTerm++;          //   - Increment currentTerm
        votedFor = self;        //   - Vote for self
        role = Role.CANDIDATE;
        leaderId = null;
        heardFromLeader = false;
        persist();
        votesGranted.clear();
        votesResponded.clear();
        votesGranted.add(self);
        electionsStarted++;
        trace("starting election");
        resetElectionTimer();   //   - Reset election timer
        if (isMajority(votesGranted.size())) {
            becomeLeader();     // single-node cluster: our own vote is a majority
            return;
        }
        sendVoteRequests();     //   - Send RequestVote RPCs to all other servers
    }

    /**
     * Sends RequestVote to every peer that has not answered yet, and keeps re-sending every
     * heartbeat interval while the election lasts. The paper (§5.1): "Servers retry RPCs if
     * they do not receive a response in a timely manner". Without retries, one lost message
     * can waste a whole election timeout on a lossy network.
     */
    private void sendVoteRequests() {
        if (!running || role != Role.CANDIDATE) {
            return;
        }
        RequestVote request = new RequestVote(currentTerm, self, log.lastIndex(), log.lastTerm(), false, false);
        for (NodeId peer : voters) {
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
        // Fig.2 Leaders: "Upon election: send initial empty AppendEntries RPCs (heartbeat) to
        // each server; repeat during idle periods to prevent election timeouts (§5.2)".
        // (The no-op entry of §5.4.2/§8 is appended here in Milestone 3.)
        sendHeartbeats();
    }

    private void sendHeartbeats() {
        if (!running || role != Role.LEADER) {
            return;
        }
        AppendEntries heartbeat = new AppendEntries(currentTerm, self, log.lastIndex(), log.lastTerm(), List.of(), 0);
        for (NodeId peer : voters) {
            if (!peer.equals(self)) {
                send(peer, heartbeat);
            }
        }
        cancel(heartbeatTimer);
        heartbeatTimer = scheduler.schedule(config.heartbeatInterval(), this::sendHeartbeats);
    }

    /** Adopts a higher term and becomes a follower (Fig.2 All Servers). */
    private void stepDown(long newTerm, String why) {
        currentTerm = newTerm;
        votedFor = null;
        leaderId = null;
        heardFromLeader = false;
        persist();
        if (role != Role.FOLLOWER) {
            becomeFollower(why);
        } else {
            trace("adopted t" + newTerm + ": " + why);
        }
    }

    private void becomeFollower(String why) {
        Role previous = role;
        role = Role.FOLLOWER;
        cancel(heartbeatTimer);
        cancel(voteRetryTimer);
        votesGranted.clear();
        votesResponded.clear();
        resetElectionTimer();
        trace("stepped down from " + previous + ": " + why);
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    private boolean believesLeaderIsAlive() {
        if (role == Role.LEADER) {
            return true;
        }
        return heardFromLeader && clock.nanos() - lastLeaderContactNanos < config.electionTimeoutMin().toNanos();
    }

    private boolean isMajority(int count) {
        return count >= voters.size() / 2 + 1;
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

    public Set<NodeId> voters() {
        return voters;
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

    public RaftLog log() {
        return log;
    }

    @Override
    public String toString() {
        return self + "(" + role + ", t" + currentTerm + ", leader=" + leaderId + ")";
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
        public Builder tracer(Tracer v) { this.tracer = v; return this; }

        public RaftNode build() {
            return new RaftNode(this);
        }
    }
}
