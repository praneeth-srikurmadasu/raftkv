package dev.raftkv.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/** Smoke test of the real file system implementation (runs on Windows, Linux and macOS). */
class RealDiskTest {

    @Test
    void writeForceRenameSyncAndReadBack() throws IOException {
        Path dir = Files.createTempDirectory("raftkv-disk-test");
        try {
            RealDisk disk = new RealDisk();
            Path tmp = dir.resolve("a.tmp");
            try (DiskFile f = disk.open(tmp)) {
                f.write(ByteBuffer.wrap("hello world".getBytes(StandardCharsets.UTF_8)), 0);
                f.truncate(5);
                f.force();
                assertThat(f.size()).isEqualTo(5L);
            }
            disk.rename(tmp, dir.resolve("a"));
            disk.syncDirectory(dir); // a no-op on Windows, a real directory fsync elsewhere
            assertThat(disk.list(dir)).isEqualTo(List.of("a"));
            try (DiskFile f = disk.open(dir.resolve("a"))) {
                assertThat(new String(f.readFully(0, 5), StandardCharsets.UTF_8)).isEqualTo("hello");
            }
            disk.delete(dir.resolve("a"));
            assertThat(disk.exists(dir.resolve("a"))).isFalse();
            assertThat(disk.list(dir.resolve("missing"))).isEmpty();
        } finally {
            try (Stream<Path> s = Files.walk(dir)) {
                for (Path p : s.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }
}
