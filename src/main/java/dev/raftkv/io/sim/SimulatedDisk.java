package dev.raftkv.io.sim;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import dev.raftkv.io.Disk;
import dev.raftkv.io.DiskException;
import dev.raftkv.io.DiskFile;

/**
 * An in-memory {@link Disk} that loses data on a crash the way real disks do (SPEC §3.7).
 *
 * <p>Every file keeps two versions of its contents: what readers currently see, and what is
 * <b>durable</b> (as of the last {@link DiskFile#force()}). Writes and truncations since the last
 * force are kept as a list of pending operations. Likewise, the directory tree keeps a durable
 * set of names, and creates/renames/deletes since the last {@link #syncDirectory} are pending.
 *
 * <p>{@link #crash(CrashMode)} rebuilds everything from the durable state and then replays each
 * pending operation according to the mode:
 * <ul>
 *   <li>{@link CrashMode#LOSE_ALL_UNSYNCED}: every pending operation is lost.</li>
 *   <li>{@link CrashMode#KEEP_ALL}: every pending operation survives (as if the OS flushed in time).</li>
 *   <li>{@link CrashMode#RANDOM}: each pending operation independently survives, is lost, or
 *       (for writes) is <b>torn</b>, only a prefix reaching the disk. This produces the nasty
 *       states real hardware produces: holes, half-written records, a rename that happened while
 *       the renamed file's contents did not.</li>
 * </ul>
 * After a crash, all previously opened handles are dead; storage code must reopen and recover.
 *
 * <p>{@link #crashAtOperation(long)} makes the N-th mutating operation throw
 * {@link SimulatedCrash} instead of running, so tests can "pull the plug" at every single step
 * of a write protocol.
 */
public final class SimulatedDisk implements Disk {

    /** What happens to operations that were not yet made durable when the power goes out. */
    public enum CrashMode { LOSE_ALL_UNSYNCED, KEEP_ALL, RANDOM }

    /** Thrown by the operation chosen with {@link #crashAtOperation}. */
    public static final class SimulatedCrash extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SimulatedCrash(String message) {
            super(message);
        }
    }

    // ------------------------------------------------------------------ model

    /** A file's contents (an "inode"), independent of its name. */
    private static final class Inode {
        byte[] data = new byte[0];
        int size;
        byte[] durableData = new byte[0];
        int durableSize;
        final List<DataOp> pending = new ArrayList<>();
    }

    private sealed interface DataOp permits WriteOp, TruncateOp {}

    private record WriteOp(long position, byte[] bytes) implements DataOp {}

    private record TruncateOp(long size) implements DataOp {}

    private sealed interface NameOp permits Create, Delete, Rename {
        String dir();
    }

    private record Create(String dir, String path, Inode inode) implements NameOp {}

    private record Delete(String dir, String path) implements NameOp {}

    private record Rename(String dir, String from, String to) implements NameOp {}

    private final Random random;
    private final Map<String, Inode> names = new HashMap<>();
    private final Map<String, Inode> durableNames = new HashMap<>();
    private final List<NameOp> pendingNames = new ArrayList<>();
    private final Set<String> directories = new HashSet<>();
    private long generation;
    private long mutatingOps;
    private long crashAtOp = -1;
    private long forces;

    public SimulatedDisk(Random random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    // ------------------------------------------------------------------ Disk API

    @Override
    public DiskFile open(Path path) {
        String p = key(path);
        Inode inode = names.get(p);
        if (inode == null) {
            mutation("create " + p);
            inode = new Inode();
            names.put(p, inode);
            pendingNames.add(new Create(parent(p), p, inode));
        }
        return new Handle(inode, generation, p);
    }

    @Override
    public boolean exists(Path path) {
        return names.containsKey(key(path));
    }

    @Override
    public List<String> list(Path dir) {
        String d = key(dir);
        List<String> out = new ArrayList<>();
        for (String p : names.keySet()) {
            if (parent(p).equals(d)) {
                out.add(p.substring(p.lastIndexOf('/') + 1));
            }
        }
        out.sort(null);
        return out;
    }

    @Override
    public void createDirectories(Path dir) {
        directories.add(key(dir));
    }

    @Override
    public void delete(Path path) {
        String p = key(path);
        if (!names.containsKey(p)) {
            return;
        }
        mutation("delete " + p);
        names.remove(p);
        pendingNames.add(new Delete(parent(p), p));
    }

    @Override
    public void rename(Path from, Path to) {
        String f = key(from);
        String t = key(to);
        Inode inode = names.get(f);
        if (inode == null) {
            throw new DiskException("rename: " + f + " does not exist");
        }
        if (!parent(f).equals(parent(t))) {
            throw new DiskException("rename across directories is not modelled: " + f + " -> " + t);
        }
        mutation("rename " + f + " -> " + t);
        names.remove(f);
        names.put(t, inode);
        pendingNames.add(new Rename(parent(f), f, t));
    }

    @Override
    public void syncDirectory(Path dir) {
        String d = key(dir);
        mutation("fsync dir " + d);
        List<NameOp> remaining = new ArrayList<>();
        for (NameOp op : pendingNames) {
            if (op.dir().equals(d)) {
                applyName(durableNames, op);
            } else {
                remaining.add(op);
            }
        }
        pendingNames.clear();
        pendingNames.addAll(remaining);
    }

    // ------------------------------------------------------------------ crash simulation

    /**
     * Power failure: rebuilds every file and the directory tree from durable state plus a
     * mode-dependent subset of pending operations. Invalidates all open handles.
     */
    public void crash(CrashMode mode) {
        Map<String, Inode> rebuilt = new HashMap<>(durableNames);
        for (NameOp op : pendingNames) {
            if (survives(mode)) {
                applyName(rebuilt, op);
            }
        }
        pendingNames.clear();
        Set<Inode> all = new HashSet<>(names.values());
        all.addAll(durableNames.values());
        all.addAll(rebuilt.values());
        for (Inode inode : all) {
            byte[] data = Arrays.copyOf(inode.durableData, inode.durableSize);
            int size = inode.durableSize;
            for (DataOp op : inode.pending) {
                switch (op) {
                    case WriteOp w -> {
                        int choice = mode == CrashMode.RANDOM ? random.nextInt(3) : (mode == CrashMode.KEEP_ALL ? 0 : 1);
                        if (choice == 1) {
                            continue; // lost
                        }
                        int len = choice == 2 ? random.nextInt(w.bytes().length + 1) : w.bytes().length; // 2 = torn
                        if (len == 0) {
                            continue;
                        }
                        int end = (int) w.position() + len;
                        if (end > data.length) {
                            data = Arrays.copyOf(data, end);
                        }
                        System.arraycopy(w.bytes(), 0, data, (int) w.position(), len);
                        size = Math.max(size, end);
                        data = Arrays.copyOf(data, size);
                    }
                    case TruncateOp t -> {
                        if (survives(mode) && t.size() < size) {
                            size = (int) t.size();
                            data = Arrays.copyOf(data, size); // bytes past the new end are gone
                        }
                    }
                }
            }
            inode.data = Arrays.copyOf(data, size);
            inode.size = size;
            inode.durableData = Arrays.copyOf(data, size);
            inode.durableSize = size;
            inode.pending.clear();
        }
        names.clear();
        names.putAll(rebuilt);
        durableNames.clear();
        durableNames.putAll(rebuilt);
        generation++;
        crashAtOp = -1;
    }

    /** The {@code n}-th mutating operation from now (1 = the next one) throws {@link SimulatedCrash}. */
    public void crashAtOperation(long n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be >= 1");
        }
        crashAtOp = mutatingOps + n;
    }

    /** Mutating operations (create, write, truncate, force, rename, delete, dir fsync) performed so far. */
    public long mutatingOperations() {
        return mutatingOps;
    }

    /** How many times a file was forced (tests check group commit with this). */
    public long forceCount() {
        return forces;
    }

    /** Flips one bit of a file, durably (simulates media corruption). */
    public void flipBit(Path path, long byteOffset, int bit) {
        Inode inode = names.get(key(path));
        if (inode == null || byteOffset >= inode.size) {
            throw new IllegalArgumentException("no byte " + byteOffset + " in " + path);
        }
        inode.data[(int) byteOffset] ^= (byte) (1 << bit);
        if (byteOffset < inode.durableSize) {
            inode.durableData[(int) byteOffset] ^= (byte) (1 << bit);
        }
    }

    // ------------------------------------------------------------------ helpers

    private boolean survives(CrashMode mode) {
        return switch (mode) {
            case KEEP_ALL -> true;
            case LOSE_ALL_UNSYNCED -> false;
            case RANDOM -> random.nextBoolean();
        };
    }

    private static void applyName(Map<String, Inode> target, NameOp op) {
        switch (op) {
            case Create c -> target.put(c.path(), c.inode());
            case Delete d -> target.remove(d.path());
            case Rename r -> {
                Inode inode = target.remove(r.from());
                if (inode != null) {
                    target.put(r.to(), inode);
                }
            }
        }
    }

    private void mutation(String what) {
        mutatingOps++;
        if (mutatingOps == crashAtOp) {
            crashAtOp = -1;
            throw new SimulatedCrash("simulated power loss before: " + what);
        }
    }

    private static String key(Path path) {
        return path.normalize().toString().replace('\\', '/');
    }

    private static String parent(String path) {
        int i = path.lastIndexOf('/');
        return i < 0 ? "" : path.substring(0, i);
    }

    private final class Handle implements DiskFile {
        private final Inode inode;
        private final long handleGeneration;
        private final String path;
        private boolean closed;

        Handle(Inode inode, long generation, String path) {
            this.inode = inode;
            this.handleGeneration = generation;
            this.path = path;
        }

        private void check() {
            if (closed) {
                throw new DiskException(path + " is closed");
            }
            if (handleGeneration != generation) {
                throw new DiskException(path + ": handle is dead (the disk crashed since it was opened)");
            }
        }

        @Override
        public long size() {
            check();
            return inode.size;
        }

        @Override
        public int read(ByteBuffer dst, long position) {
            check();
            if (position >= inode.size) {
                return 0;
            }
            int n = (int) Math.min(dst.remaining(), inode.size - position);
            dst.put(inode.data, (int) position, n);
            return n;
        }

        @Override
        public void write(ByteBuffer src, long position) {
            check();
            mutation("write " + path + "@" + position);
            byte[] bytes = new byte[src.remaining()];
            src.get(bytes);
            int end = (int) position + bytes.length;
            if (end > inode.data.length) {
                inode.data = Arrays.copyOf(inode.data, Math.max(end, inode.data.length * 2));
            }
            if (position > inode.size) {
                Arrays.fill(inode.data, inode.size, (int) position, (byte) 0);
            }
            System.arraycopy(bytes, 0, inode.data, (int) position, bytes.length);
            inode.size = Math.max(inode.size, end);
            inode.pending.add(new WriteOp(position, bytes));
        }

        @Override
        public void truncate(long size) {
            check();
            if (size >= inode.size) {
                return;
            }
            mutation("truncate " + path + " to " + size);
            inode.size = (int) size;
            inode.pending.add(new TruncateOp(size));
        }

        @Override
        public void force() {
            check();
            mutation("fsync " + path);
            forces++;
            inode.durableData = Arrays.copyOf(inode.data, inode.size);
            inode.durableSize = inode.size;
            inode.pending.clear();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
