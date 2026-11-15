package dev.raftkv.raft;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import dev.raftkv.codec.BinaryReader;
import dev.raftkv.codec.BinaryWriter;
import dev.raftkv.codec.CodecException;
import dev.raftkv.common.Bytes;
import dev.raftkv.common.NodeId;

/**
 * A Raft group's membership (thesis ch. 4): the voters, whose majority decides elections and
 * commits, and the learners, which receive the log but don't vote and aren't counted.
 *
 * <p>Changes go one server at a time ({@link #isSingleServerChangeTo}): add a learner, promote a
 * learner to voter, or remove a server. With single-server changes any majority of the old voters
 * and any majority of the new voters overlap, so two leaders can never be elected in the same
 * term, even while servers disagree about which configuration is current.
 */
public record Membership(SortedSet<NodeId> voters, SortedSet<NodeId> learners) {

    public Membership {
        voters = Collections.unmodifiableSortedSet(new TreeSet<>(voters));
        learners = Collections.unmodifiableSortedSet(new TreeSet<>(learners));
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("a group needs at least one voter");
        }
        for (NodeId l : learners) {
            if (voters.contains(l)) {
                throw new IllegalArgumentException(l + " can't be both a voter and a learner");
            }
        }
    }

    public static Membership of(java.util.Collection<NodeId> voters) {
        return new Membership(new TreeSet<>(voters), new TreeSet<>());
    }

    public boolean isVoter(NodeId id) {
        return voters.contains(id);
    }

    public boolean isLearner(NodeId id) {
        return learners.contains(id);
    }

    public boolean isMember(NodeId id) {
        return voters.contains(id) || learners.contains(id);
    }

    /** Voters and learners, in id order. */
    public SortedSet<NodeId> members() {
        TreeSet<NodeId> all = new TreeSet<>(voters);
        all.addAll(learners);
        return Collections.unmodifiableSortedSet(all);
    }

    /** Smallest number of voters that is a majority. */
    public int quorum() {
        return voters.size() / 2 + 1;
    }

    public Membership withLearner(NodeId id) {
        if (isMember(id)) {
            throw new IllegalArgumentException(id + " is already a member");
        }
        TreeSet<NodeId> l = new TreeSet<>(learners);
        l.add(id);
        return new Membership(voters, l);
    }

    public Membership promoted(NodeId id) {
        if (!isLearner(id)) {
            throw new IllegalArgumentException(id + " is not a learner");
        }
        TreeSet<NodeId> v = new TreeSet<>(voters);
        TreeSet<NodeId> l = new TreeSet<>(learners);
        l.remove(id);
        v.add(id);
        return new Membership(v, l);
    }

    public Membership without(NodeId id) {
        if (!isMember(id)) {
            throw new IllegalArgumentException(id + " is not a member");
        }
        TreeSet<NodeId> v = new TreeSet<>(voters);
        TreeSet<NodeId> l = new TreeSet<>(learners);
        v.remove(id);
        l.remove(id);
        return new Membership(v, l);
    }

    /**
     * True if {@code next} differs from this by exactly one server: one learner added, one learner
     * promoted, or one server (voter or learner) removed.
     */
    public boolean isSingleServerChangeTo(Membership next) {
        TreeSet<NodeId> changed = new TreeSet<>();
        for (NodeId id : members()) {
            boolean sameRole = (isVoter(id) && next.isVoter(id)) || (isLearner(id) && next.isLearner(id));
            if (!sameRole) {
                changed.add(id);
            }
        }
        for (NodeId id : next.members()) {
            if (!isMember(id)) {
                changed.add(id);
            }
        }
        if (changed.size() != 1) {
            return false;
        }
        NodeId id = changed.first();
        boolean added = !isMember(id) && next.isLearner(id);
        boolean promoted = isLearner(id) && next.isVoter(id);
        boolean removed = isMember(id) && !next.isMember(id);
        return added || promoted || removed;
    }

    public Bytes encode() {
        BinaryWriter w = new BinaryWriter();
        writeTo(w);
        return w.toBytes();
    }

    public void writeTo(BinaryWriter w) {
        w.writeInt(voters.size());
        for (NodeId v : voters) {
            w.writeInt(v.value());
        }
        w.writeInt(learners.size());
        for (NodeId l : learners) {
            w.writeInt(l.value());
        }
    }

    public static Membership decode(Bytes bytes) {
        BinaryReader r = new BinaryReader(bytes);
        Membership m = readFrom(r);
        r.expectEnd();
        return m;
    }

    public static Membership readFrom(BinaryReader r) {
        TreeSet<NodeId> v = readIds(r);
        TreeSet<NodeId> l = readIds(r);
        try {
            return new Membership(v, l);
        } catch (IllegalArgumentException e) {
            throw new CodecException("invalid membership: " + e.getMessage());
        }
    }

    private static TreeSet<NodeId> readIds(BinaryReader r) {
        int n = r.readInt();
        if (n < 0 || n > r.remaining() / 4) {
            throw new CodecException("invalid member count " + n);
        }
        TreeSet<NodeId> ids = new TreeSet<>();
        for (int i = 0; i < n; i++) {
            int id = r.readInt();
            if (id < 0) {
                throw new CodecException("invalid node id " + id);
            }
            ids.add(new NodeId(id));
        }
        return ids;
    }

    @Override
    public String toString() {
        return "voters " + voters + (learners.isEmpty() ? "" : ", learners " + learners);
    }
}
