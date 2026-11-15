package dev.raftkv.client;

import java.util.List;
import java.util.Objects;

import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;

/**
 * Where a request should go: a Raft group and the servers that host it.
 *
 * @param group   the group
 * @param servers its servers (any of them works as a first guess; non-leaders answer with a hint)
 */
public record Route(GroupId group, List<NodeId> servers) {

    public Route {
        Objects.requireNonNull(group, "group");
        servers = List.copyOf(servers);
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("a route needs at least one server");
        }
    }
}
