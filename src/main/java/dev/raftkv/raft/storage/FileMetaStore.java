package dev.raftkv.raft.storage;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import dev.raftkv.common.NodeId;
import dev.raftkv.io.Disk;
import dev.raftkv.io.DiskFile;

/**
 * Stores {@code currentTerm} and {@code votedFor} in a small file, crash-safely (SPEC §3.7).
 *
 * <p>File format (25 bytes): {@code [magic "RKVM":4][version:1][term:8][hasVote:1][votedFor:4][crc32c:4]}.
 *
 * <p>Save protocol, and why each step exists:
 * <ol>
 *   <li><b>Write {@code meta.tmp}</b>, never the live file in place. A crash in the middle of an
 *       in-place write leaves a torn mix of old and new bytes: neither state, and unreadable.</li>
 *   <li><b>fsync {@code meta.tmp}</b>, so its contents are on the disk before the next step makes
 *       it the official copy. Otherwise the rename could survive a crash while the data didn't.</li>
 *   <li><b>Atomically rename</b> {@code meta.tmp} over {@code meta}. After a crash, readers see the
 *       complete old file or the complete new file, never half of each.</li>
 *   <li><b>fsync the directory</b>. The rename is a change to the directory; until the
 *       directory is synced, a crash can undo it, silently bringing back the old term and vote.
 *       That is exactly the "forgot my vote" bug.</li>
 * </ol>
 * Only after step 4 may Raft send a message that depends on the new state.
 *
 * <p>The CRC detects media corruption; a corrupt meta file is a fatal error, because guessing a
 * term or vote could break Election Safety.
 */
public final class FileMetaStore implements MetaStore {

    private static final int MAGIC = 0x524B564D; // "RKVM"
    private static final byte VERSION = 1;
    private static final int SIZE = 4 + 1 + 8 + 1 + 4 + 4;

    private final Disk disk;
    private final Path dir;
    private final Path file;
    private final Path tmp;

    public FileMetaStore(Disk disk, Path dir) {
        this.disk = disk;
        this.dir = dir;
        this.file = dir.resolve("meta");
        this.tmp = dir.resolve("meta.tmp");
        disk.createDirectories(dir);
    }

    @Override
    public PersistentState load() {
        if (disk.exists(tmp)) {
            disk.delete(tmp); // leftover from a save that crashed before its rename: ignore it
        }
        if (!disk.exists(file)) {
            return PersistentState.INITIAL;
        }
        byte[] bytes;
        try (DiskFile f = disk.open(file)) {
            if (f.size() != SIZE) {
                throw new CorruptStateException("meta file has " + f.size() + " bytes, expected " + SIZE);
            }
            bytes = f.readFully(0, SIZE);
        }
        ByteBuffer b = ByteBuffer.wrap(bytes);
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, SIZE - 4);
        if (b.getInt(SIZE - 4) != (int) crc.getValue()) {
            throw new CorruptStateException("meta file CRC mismatch");
        }
        if (b.getInt() != MAGIC || b.get() != VERSION) {
            throw new CorruptStateException("meta file has wrong magic/version");
        }
        long term = b.getLong();
        boolean hasVote = b.get() == 1;
        int vote = b.getInt();
        return new PersistentState(term, hasVote ? new NodeId(vote) : null);
    }

    @Override
    public void save(PersistentState state) {
        ByteBuffer b = ByteBuffer.allocate(SIZE);
        b.putInt(MAGIC).put(VERSION).putLong(state.currentTerm());
        b.put((byte) (state.votedFor() != null ? 1 : 0)).putInt(state.votedFor() != null ? state.votedFor().value() : 0);
        CRC32C crc = new CRC32C();
        crc.update(b.array(), 0, SIZE - 4);
        b.putInt((int) crc.getValue());
        b.flip();
        try (DiskFile f = disk.open(tmp)) {
            f.truncate(0);
            f.write(b, 0);  // 1. write the temp file
            f.force();      // 2. make its contents durable
        }
        disk.rename(tmp, file); // 3. atomically replace the live file
        disk.syncDirectory(dir); // 4. make the rename itself durable
    }
}
