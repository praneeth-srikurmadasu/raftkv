package dev.raftkv.common;

/**
 * Identifies one Raft group. Group 0 is the ShardController (SPEC §6.3); data groups are 1, 2, ...
 *
 * <p>Every network frame carries a group id so that one TCP connection between two processes
 * can carry the traffic of all the groups they share (SPEC §5.2, multi-Raft).
 */
public record GroupId(int value) implements Comparable<GroupId> {

    /** The ShardController group. */
    public static final GroupId CONTROLLER = new GroupId(0);

    public GroupId {
        if (value < 0) {
            throw new IllegalArgumentException("group id must be >= 0, was " + value);
        }
    }

    public static GroupId of(int value) {
        return new GroupId(value);
    }

    @Override
    public int compareTo(GroupId other) {
        return Integer.compare(value, other.value);
    }

    @Override
    public String toString() {
        return "g" + value;
    }
}
