package dev.raftkv.raft.storage;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

import dev.raftkv.common.Bytes;
import dev.raftkv.io.Disk;
import dev.raftkv.io.DiskFile;

/**
 * Keeps the latest snapshot in one file, written crash-safely like the meta file (SPEC §3.8).
 *
 * <p>Format: {@code [magic "RKVS":4][version:1][lastIncludedIndex:8][lastIncludedTerm:8]
 * [length:4][data:length][crc32c:4]}, CRC over everything before it.
 *
 * <p>Save protocol: write {@code snapshot.tmp}, fsync it, rename it over {@code snapshot}, fsync
 * the directory. A crash at any point leaves either the complete old snapshot or the complete new
 * one. Only after {@link #save} returns does Raft delete the log entries the snapshot covers.
 *
 * <p>A snapshot file that fails its CRC is a fatal error ({@link CorruptStateException}): the log
 * entries it replaced are gone, so there is nothing to fall back to.
 */
public final class FileSnapshotStore implements SnapshotStore {

    private static final int MAGIC = 0x524B5653; // "RKVS"
    private static final byte VERSION = 1;
    private static final int HEADER = 4 + 1 + 8 + 8 + 4;

    private final Disk disk;
    private final Path dir;
    private final Path file;
    private final Path tmp;

    public FileSnapshotStore(Disk disk, Path dir) {
        this.disk = Objects.requireNonNull(disk, "disk");
        this.dir = dir;
        this.file = dir.resolve("snapshot");
        this.tmp = dir.resolve("snapshot.tmp");
        disk.createDirectories(dir);
    }

    @Override
    public Optional<Snapshot> load() {
        if (disk.exists(tmp)) {
            disk.delete(tmp); // a save that crashed before its rename: the old snapshot is still current
        }
        if (!disk.exists(file)) {
            return Optional.empty();
        }
        byte[] bytes;
        try (DiskFile f = disk.open(file)) {
            long size = f.size();
            if (size < HEADER + 4 || size > Integer.MAX_VALUE) {
                throw new CorruptStateException("snapshot file has an impossible size: " + size);
            }
            bytes = f.readFully(0, (int) size);
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer b = ByteBuffer.wrap(bytes);
        if (b.getInt(bytes.length - 4) != (int) crc.getValue()) {
            throw new CorruptStateException("snapshot file CRC mismatch");
        }
        if (b.getInt() != MAGIC || b.get() != VERSION) {
            throw new CorruptStateException("snapshot file has wrong magic/version");
        }
        long index = b.getLong();
        long term = b.getLong();
        int length = b.getInt();
        if (length != bytes.length - HEADER - 4) {
            throw new CorruptStateException("snapshot length field " + length + " does not match the file");
        }
        try {
            return Optional.of(new Snapshot(index, term, Bytes.copyOf(bytes, HEADER, length)));
        } catch (IllegalArgumentException e) {
            throw new CorruptStateException("snapshot header is invalid: " + e.getMessage());
        }
    }

    @Override
    public void save(Snapshot s) {
        int length = s.data().length();
        ByteBuffer b = ByteBuffer.allocate(HEADER + length + 4);
        b.putInt(MAGIC).put(VERSION).putLong(s.lastIncludedIndex()).putLong(s.lastIncludedTerm()).putInt(length);
        s.data().writeTo(b);
        CRC32C crc = new CRC32C();
        crc.update(b.array(), 0, HEADER + length);
        b.putInt((int) crc.getValue());
        b.flip();
        try (DiskFile f = disk.open(tmp)) {
            f.truncate(0);
            f.write(b, 0); // 1. write the temp file
            f.force();     // 2. make its contents durable
        }
        disk.rename(tmp, file);  // 3. atomically replace the old snapshot
        disk.syncDirectory(dir); // 4. make the rename durable before any log entry is deleted
    }
}
