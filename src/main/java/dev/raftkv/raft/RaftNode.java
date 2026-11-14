package dev.raftkv.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
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
import dev.raftkv.protocol.RequestVote;
import dev.raftkv.protocol.RequestVoteReply;
import dev.raftkv.protocol.TimeoutNow;
import dev.raftkv.raft.log.EntryType;
import dev.raftkv.raft.log.LogEntry;
import dev.raftkv.raft.log.RaftLog;
import dev.raftkv.raft.storage.MetaStore;
import dev.raftkv.raft.storage.PersistentState;
import dev.raftkv.transport.Envelope;
import dev.raftkv.transport.Transport;

/**
 * One server's replica of one Raft group.
 *
 * <p><b>Scope so far:</b> leader election (Milestone 2) and log replication (Milestone 3):
 * AppendEntries with the full Figure 2 receiver rules, fast backtracking, the commit rule that
 * only counts replicas for entries of the current term (§5.4.2, Figure 8), a no-op entry on
 * election, and applying committed entries to the {@link StateMachine} in order.
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
    private final StateMachine stateMachine;
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
        this.stateMachine = b.stateMachine == null ? (index, command) -> Bytes.EMPTY : b.stateMachine;
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
        commitIndex = 0;   // volatile: relearned from the leader
        lastApplied = 0;   // volatile: the state machine is rebuilt by re-applying the log
        trace("started (votedFor=" + votedFor + ", last log " + log.lastIndex() + "@t" + log.lastTerm() + ")");
        resetElectionTimer();
    }

    /** Stops all activity; volatile state is lost, like a crash. Persistent state stays in the stores. */
    public void stop() {
        running = false;
        cancel(electionTimer);
        cancel(heartbeatTimer);
        cancel(voteRetryTimer);
        failPending();
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
        // Fig.2 Leaders: "If command received from client: append entry to local log, respond
        // after entry applied to state machine (§5.3)".
        LogEntry entry = new LogEntry(log.lastIndex() + 1, currentTerm, EntryType.COMMAND, command);
        log.append(List.of(entry));
        CompletableFuture<CommitResult> future = new CompletableFuture<>();
        pending.put(entry.index(), new Pending(entry.term(), future));
        advanceCommitIndex(); // a single-node group commits immediately
        replicateToAll();
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

        long prevIndex = ae.prevLogIndex();
        // Fig.2 AppendEntries rule 2: "Reply false if log doesn't contain an entry at
        // prevLogIndex whose term matches prevLogTerm (§5.3)".
        if (!log.contains(prevIndex)) {
            // Fast backtracking (§5.3): our log is too short; retry from just past our end.
            send(from, new AppendEntriesReply(currentTerm, false, 0, log.lastIndex() + 1, 0));
            return;
        }
        long termAtPrev = log.termAt(prevIndex);
        if (termAtPrev != ae.prevLogTerm()) {
            // Fast backtracking: report the conflicting term and the first index we hold for it,
            // so the leader can skip the whole term in one round trip.
            send(from, new AppendEntriesReply(currentTerm, false, 0, firstIndexOfTerm(termAtPrev, prevIndex), termAtPrev));
            return;
        }

        appendNewEntries(ae);

        long lastNewIndex = prevIndex + ae.entries().size();
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
        send(from, new AppendEntriesReply(currentTerm, true, lastNewIndex, 0, 0));
    }

    /** Fig.2 AppendEntries rules 3 and 4. */
    private void appendNewEntries(AppendEntries ae) {
        List<LogEntry> entries = ae.entries();
        if (config.has(Bug.TRUNCATE_ON_STALE_APPEND)) {
            if (log.lastIndex() > ae.prevLogIndex()) {
                log.truncateSuffix(ae.prevLogIndex() + 1);
            }
            log.append(entries);
            return;
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
                log.truncateSuffix(e.index());
            }
            // Rule 4: "Append any new entries not already in the log".
            log.append(entries.subList(i, entries.size()));
            return;
        }
    }

    /** Smallest index of the run of {@code term} entries that ends at or before {@code fromIndex}. */
    private long firstIndexOfTerm(long term, long fromIndex) {
        long i = fromIndex;
        while (i > 1 && log.termAt(i - 1) == term) {
            i--;
        }
        return i;
    }

    // ---------------------------------------------------------------- AppendEntries (leader side)

    private void onAppendEntriesReply(NodeId from, AppendEntriesReply reply) {
        if (role != Role.LEADER || reply.term() != currentTerm) {
            return; // stale reply (a higher term has already been handled in handle())
        }
        if (reply.success()) {
            if (reply.matchIndex() > log.lastIndex()) {
                throw new IllegalStateException(from + " claims to match index " + reply.matchIndex()
                        + " beyond our last index " + log.lastIndex());
            }
            long matchBefore = leaderState.matchIndex(from);
            leaderState.recordMatch(from, reply.matchIndex());
            advanceCommitIndex();
            // Flow control: only a reply that made PROGRESS keeps the entries flowing. Replies to
            // older, overlapping or duplicated messages don't trigger sends; otherwise every
            // duplicate would start another never-ending chain of messages (SPEC §3.5).
            if (reply.matchIndex() > matchBefore && leaderState.nextIndex(from) <= log.lastIndex()) {
                replicateTo(from);
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
            replicateTo(from); // retry at once from the better position
        }
        // A rejection that teaches us nothing new (stale or duplicated) sends nothing; the next
        // heartbeat retries anyway.
    }

    /** Last index in our log whose term is {@code term}, or 0 if we have none. */
    private long lastIndexOfTerm(long term) {
        for (long i = log.lastIndex(); i >= 1; i--) {
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
        for (NodeId peer : voters) {
            if (!peer.equals(self)) {
                replicateTo(peer);
            }
        }
    }

    /**
     * Sends the next batch of entries (possibly none: a heartbeat) to {@code peer}, starting at
     * its nextIndex. Messages may overlap or arrive twice; followers handle that idempotently.
     */
    private void replicateTo(NodeId peer) {
        long next = leaderState.nextIndex(peer);
        long prevIndex = next - 1;
        List<LogEntry> entries = next <= log.lastIndex()
                ? log.entriesFrom(next, config.maxEntriesPerAppend())
                : List.of();
        send(peer, new AppendEntries(currentTerm, self, prevIndex, log.termAt(prevIndex), entries, commitIndex));
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
        matches.add(log.lastIndex()); // the leader's own log
        for (long m : leaderState.matchIndexes()) {
            matches.add(m);
        }
        matches.sort(Collections.reverseOrder());
        long n = matches.get(voters.size() / 2); // highest index stored on a majority
        if (n <= commitIndex) {
            return;
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
        // Fig.2: nextIndex starts at leader last log index + 1, matchIndex at 0.
        List<NodeId> peers = voters.stream().filter(p -> !p.equals(self)).toList();
        leaderState = new LeaderState(peers, log.lastIndex());
        // §8: "each leader commit a blank no-op entry into the log at the start of its term".
        // Entries from earlier terms can only commit indirectly, through an entry of the
        // current term (Figure 8), so without this they could wait forever for a client write.
        if (!config.has(Bug.NO_NOOP_ON_ELECTION)) {
            log.append(List.of(LogEntry.noop(log.lastIndex() + 1, currentTerm)));
        }
        advanceCommitIndex(); // a single-node group commits the no-op at once
        // Fig.2 Leaders: "Upon election: send initial empty AppendEntries RPCs (heartbeat) to
        // each server; repeat during idle periods to prevent election timeouts (§5.2)".
        sendHeartbeats();
    }

    private void sendHeartbeats() {
        if (!running || role != Role.LEADER) {
            return;
        }
        replicateToAll(); // carries entries to followers that are behind; empty heartbeat otherwise
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
        leaderState = null;
        failPending();
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
        public Builder tracer(Tracer v) { this.tracer = v; return this; }

        public RaftNode build() {
            return new RaftNode(this);
        }
    }
}
