package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.List;

/**
 * Answer to {@link DeleteShard}.
 *
 * @param configNum echoes the request
 * @param done      the shards are deleted (now, or already earlier)
 * @param shards    echoes the request
 */
public record DeleteShardReply(long configNum, boolean done, List<Integer> shards) implements ShardMessage {

    public DeleteShardReply {
        nonNegative(configNum, "configNum");
        shards = ShardMessage.checkedShards(shards);
    }
}
