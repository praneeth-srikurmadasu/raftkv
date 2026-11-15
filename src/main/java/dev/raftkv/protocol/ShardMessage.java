package dev.raftkv.protocol;

import java.util.List;

/**
 * Messages between replica groups for moving shards (SPEC §6.6). They are sent between the
 * servers of two groups; the envelope's group is the <em>receiving</em> group, and every request
 * names the group that sent it, so the reply can be addressed back.
 */
public sealed interface ShardMessage extends Message permits FetchShard, FetchShardReply, DeleteShard, DeleteShardReply {

    /** The configuration the request or reply is about. */
    long configNum();

    static List<Integer> checkedShards(List<Integer> shards) {
        List<Integer> copy = List.copyOf(shards);
        for (int s : copy) {
            if (s < 0) {
                throw new IllegalArgumentException("negative shard " + s);
            }
        }
        return copy;
    }
}
