package dev.raftkv.common;

/**
 * Identifies one server process in the cluster.
 *
 * <p>A plain non-negative {@code int} keeps the wire format small and comparisons cheap.
 * It prints as {@code n3}; the CLI can map ids to friendly names later.
 */
public record NodeId(int value) implements Comparable<NodeId> {

    public NodeId {
        if (value < 0) {
            throw new IllegalArgumentException("node id must be >= 0, was " + value);
        }
    }

    public static NodeId of(int value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(NodeId other) {
        return Integer.compare(value, other.value);
    }

    @Override
    public String toString() {
        return "n" + value;
    }
}
