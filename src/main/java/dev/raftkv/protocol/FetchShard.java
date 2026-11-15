package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.List;

/**
 * "Send me these shards as they were frozen by configuration {@code configNum}" (SPEC §6.6 step 1).
 * Sent by the new owner's leader to the servers of the previous owner.
 *
 * @param configNum      the configuration that moved the shards
 * @param requesterGroup the group that asks (the reply goes to it)
 * @param shards         the shard numbers
 */
public record FetchShard(long configNum, int requesterGroup, List<Integer> shards) implements ShardMessage {

    public FetchShard {
        nonNegative(configNum, "configNum");
        nonNegative(requesterGroup, "requesterGroup");
        shards = ShardMessage.checkedShards(shards);
    }
}
