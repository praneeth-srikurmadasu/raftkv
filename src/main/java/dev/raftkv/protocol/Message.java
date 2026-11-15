package dev.raftkv.protocol;

/**
 * Every message that can travel between RaftKV processes: Raft's own messages between servers
 * ({@link RaftMessage}), client requests/replies ({@link ClientRequest}, {@link ClientReply}), and
 * shard moves between replica groups ({@link ShardMessage}).
 *
 * <p>{@code sealed} means the compiler knows the complete list of message types. A
 * {@code switch} over a {@code Message} with no {@code default} branch is checked for
 * exhaustiveness: add a new message type and every switch that forgot to handle it stops
 * compiling, including the codec. That turns "forgot to serialize the new message" from a
 * runtime bug into a compile error.
 *
 * <p>Clients match replies to requests by {@code (clientId, sequence)}, which the reply echoes.
 */
public sealed interface Message permits RaftMessage, ClientRequest, ClientReply, ShardMessage {

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
            case ClientRequest m -> MessageType.CLIENT_REQUEST;
            case ClientReply m -> MessageType.CLIENT_REPLY;
            case FetchShard m -> MessageType.FETCH_SHARD;
            case FetchShardReply m -> MessageType.FETCH_SHARD_REPLY;
            case DeleteShard m -> MessageType.DELETE_SHARD;
            case DeleteShardReply m -> MessageType.DELETE_SHARD_REPLY;
        };
    }
}
