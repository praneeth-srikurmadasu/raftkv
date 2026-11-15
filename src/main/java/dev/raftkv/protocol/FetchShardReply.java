package dev.raftkv.protocol;

import static dev.raftkv.protocol.Checks.nonNegative;

import java.util.Objects;

import dev.raftkv.common.Bytes;

/**
 * Answer to {@link FetchShard}.
 *
 * @param configNum echoes the request
 * @param ready     false if the sender has not applied {@code configNum} yet (try again later)
 * @param shards    when ready: the encoded shard data, including each shard's session table
 */
public record FetchShardReply(long configNum, boolean ready, Bytes shards) implements ShardMessage {

    public FetchShardReply {
        nonNegative(configNum, "configNum");
        Objects.requireNonNull(shards, "shards");
    }
}
