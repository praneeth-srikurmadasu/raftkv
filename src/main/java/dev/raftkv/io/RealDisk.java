package dev.raftkv.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * {@link Disk} backed by the real file system through {@link FileChannel}.
 *
 * <p>Directory fsync: on Linux and macOS a directory can be opened and forced, which makes
 * renames and new file names durable. Windows does not allow opening a directory as a file
 * channel; NTFS journals metadata changes itself, so on Windows the call is a no-op.
 */
public final class RealDisk implements Disk {

    private static final boolean WINDOWS = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

    /** The caller owns the returned file and must close it. */
    @Override
    @SuppressWarnings("resource")
    public DiskFile open(Path path) {
        try {
            return new RealFile(FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));
        } catch (IOException e) {
            throw new DiskException("cannot open " + path, e);
        }
    }

    @Override
    public boolean exists(Path path) {
        return Files.exists(path);
    }

    @Override
    public List<String> list(Path dir) {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).sorted().toList();
        } catch (NoSuchFileException e) {
            return List.of();
        } catch (IOException e) {
            throw new DiskException("cannot list " + dir, e);
        }
    }

    @Override
    public void createDirectories(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new DiskException("cannot create " + dir, e);
        }
    }

    @Override
    public void delete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new DiskException("cannot delete " + path, e);
        }
    }

    @Override
    public void rename(Path from, Path to) {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new DiskException("cannot rename " + from + " to " + to, e);
        }
    }

    @Override
    public void syncDirectory(Path dir) {
        if (WINDOWS) {
            return; // not supported (and not needed) on NTFS
        }
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            throw new DiskException("cannot fsync directory " + dir, e);
        }
    }

    private static final class RealFile implements DiskFile {
        private final FileChannel ch;

        RealFile(FileChannel ch) {
            this.ch = ch;
        }

        @Override
        public long size() {
            try {
                return ch.size();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public int read(ByteBuffer dst, long position) {
            try {
                int n = ch.read(dst, position);
                return Math.max(n, 0);
            } catch (IOException e) {
                throw new DiskException("read failed", e);
            }
        }

        @Override
        public void write(ByteBuffer src, long position) {
            try {
                long pos = position;
                while (src.hasRemaining()) {
                    pos += ch.write(src, pos);
                }
            } catch (IOException e) {
                throw new DiskException("write failed", e);
            }
        }

        @Override
        public void truncate(long size) {
            try {
                ch.truncate(size);
            } catch (IOException e) {
                throw new DiskException("truncate failed", e);
            }
        }

        @Override
        public void force() {
            try {
                ch.force(false); // data + the metadata needed to read it back (fdatasync on Linux)
            } catch (IOException e) {
                throw new DiskException("fsync failed", e);
            }
        }

        @Override
        public void close() {
            try {
                ch.close();
            } catch (IOException e) {
                throw new DiskException("close failed", e);
            }
        }
    }
}
