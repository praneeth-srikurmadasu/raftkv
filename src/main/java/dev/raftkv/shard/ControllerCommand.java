package dev.raftkv.shard;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import dev.raftkv.common.NodeId;

/**
 * Operations on the shard controller (SPEC §6.3). All of them go through the controller's Raft
 * log with a client session, so a retried Join cannot create two configurations.
 */
public sealed interface ControllerCommand {

    /** Adds replica groups (gid → servers) and rebalances; an existing gid gets the new server list. */
    record Join(SortedMap<Integer, List<NodeId>> groups) implements ControllerCommand {
        public Join {
            if (groups.isEmpty()) {
                throw new IllegalArgumentException("join needs at least one group");
            }
            TreeMap<Integer, List<NodeId>> copy = new TreeMap<>();
            for (Map.Entry<Integer, List<NodeId>> e : groups.entrySet()) {
                if (e.getKey() < 1) {
                    throw new IllegalArgumentException("data group ids start at 1, got " + e.getKey());
                }
                if (e.getValue().isEmpty()) {
                    throw new IllegalArgumentException("group " + e.getKey() + " needs at least one server");
                }
                copy.put(e.getKey(), List.copyOf(e.getValue()));
            }
            groups = java.util.Collections.unmodifiableSortedMap(copy);
        }
    }

    /** Removes replica groups; their shards move to the remaining groups. */
    record Leave(List<Integer> gids) implements ControllerCommand {
        public Leave {
            gids = List.copyOf(gids);
            if (gids.isEmpty()) {
                throw new IllegalArgumentException("leave needs at least one group");
            }
        }
    }

    /** Moves one shard to a group, without rebalancing anything else. */
    record Move(int shard, int gid) implements ControllerCommand {
        public Move {
            if (shard < 0 || gid < 1) {
                throw new IllegalArgumentException("bad move of shard " + shard + " to group " + gid);
            }
        }
    }

    /** Returns configuration {@code num}, or the latest one if {@code num} is -1 or beyond the latest. */
    record Query(long num) implements ControllerCommand {
        /** Ask for the latest configuration. */
        public static final long LATEST = -1;
    }
}
