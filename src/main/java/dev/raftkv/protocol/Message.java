package dev.raftkv.protocol;

/**
 * Every message that can travel between RaftKV processes.
 *
 * <p>{@code sealed} means the compiler knows the complete list of message types. A
 * {@code switch} over a {@code Message} with no {@code default} branch is checked for
 * exhaustiveness: add a new message type and every switch that forgot to handle it stops
 * compiling, including the codec. That turns "forgot to serialize the new message" from a
 * runtime bug into a compile error.
 *
 * <p>Raft messages are plain one-way messages: a reply is just another message sent back
 * (the style etcd/raft uses). Raft tolerates loss, duplication and reordering by design,
 * so the Raft core needs no request/response futures. Client RPCs will add correlation ids
 * on top (Milestone 5/7).
 */
public sealed interface Message
        permits RequestVote, RequestVoteReply,
                AppendEntries, AppendEntriesReply,
                InstallSnapshot, InstallSnapshotReply,
                TimeoutNow {

    /** Every Raft message carries the sender's current term (Figure 2, "Rules for Servers"). */
    long term();

    /** The wire type of this message. Exhaustive switch: no default needed. */
    default MessageType type() {
        return switch (this) {
            case RequestVote m -> MessageType.REQUEST_VOTE;
            case RequestVoteReply m -> MessageType.REQUEST_VOTE_REPLY;
            case AppendEntries m -> MessageType.APPEND_ENTRIES;
            case AppendEntriesReply m -> MessageType.APPEND_ENTRIES_REPLY;
            case InstallSnapshot m -> MessageType.INSTALL_SNAPSHOT;
            case InstallSnapshotReply m -> MessageType.INSTALL_SNAPSHOT_REPLY;
            case TimeoutNow m -> MessageType.TIMEOUT_NOW;
        };
    }
}
