package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.List;

/**
 * "I have installed these shards for {@code configNum}; you can delete your frozen copy"
 * (SPEC §6.6 step 3, garbage collection). Sent by the new owner's leader to the previous owner.
 *
 * @param configNum      the configuration that moved the shards
 * @param requesterGroup the group that asks (the reply goes to it)
 * @param shards         the shard numbers
 */
public record DeleteShard(long configNum, int requesterGroup, List<Integer> shards) implements ShardMessage {

    public DeleteShard {
        nonNegative(configNum, "configNum");
        nonNegative(requesterGroup, "requesterGroup");
        shards = ShardMessage.checkedShards(shards);
    }
}
