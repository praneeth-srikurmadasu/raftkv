package dev.raftkv.protocol;

/**
 * A message of the Raft protocol itself, exchanged between servers of one group.
 *
 * <p>Raft messages are plain one-way messages: a reply is just another message sent back
 * (the style etcd/raft uses). Raft tolerates loss, duplication and reordering by design, so
 * the Raft core needs no request/response futures.
 */
public sealed interface RaftMessage extends Message
        permits RequestVote, RequestVoteReply,
                AppendEntries, AppendEntriesReply,
                InstallSnapshot, InstallSnapshotReply,
                TimeoutNow {

    /** Every Raft message carries the sender's current term (Figure 2, "Rules for Servers"). */
    long term();
}
