package dev.raftkv.io;

import java.nio.file.Path;
import java.util.List;

/**
 * The file-system operations RaftKV's storage needs, and nothing more (SPEC §3.7).
 *
 * <p>All persistent state goes through this interface so tests can swap the real disk for
 * {@link dev.raftkv.io.sim.SimulatedDisk}, which models what a real disk does on power loss:
 * writes that were not forced may be lost or torn, and directory changes (creating, renaming,
 * deleting files) that were not synced may be undone.
 *
 * <p>The durability rules callers must follow, exactly as on a real POSIX system:
 * <ul>
 *   <li>File contents are durable only after {@link DiskFile#force()}.</li>
 *   <li>A file's <em>name</em> (create, rename, delete) is durable only after
 *       {@link #syncDirectory(Path)} on its parent directory.</li>
 * </ul>
 */
public interface Disk {

    /** Opens {@code path} for reading and writing, creating an empty file if it doesn't exist. */
    DiskFile open(Path path);

    boolean exists(Path path);

    /** Names (not full paths) of the files directly inside {@code dir}, sorted. */
    List<String> list(Path dir);

    /** Creates the directory and any missing parents (treated as durable immediately). */
    void createDirectories(Path dir);

    /** Deletes the file if it exists. */
    void delete(Path path);

    /** Atomically renames {@code from} to {@code to}, replacing {@code to} if it exists. */
    void rename(Path from, Path to);

    /** Makes create/rename/delete operations inside {@code dir} durable (fsync of the directory). */
    void syncDirectory(Path dir);
}
