import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Unpacks a RaftKV bundle file into a folder.
 *
 * <p>Run it straight from source (Java 11+ can run a single .java file):
 * <pre>
 *   java Unbundle.java &lt;bundle-file&gt; [target-dir] [--force] [--dry-run]
 * </pre>
 *
 * <p>Bundle format (plain text, UTF-8):
 * <pre>
 *   # RAFTKV-BUNDLE v1
 *   # name: phase0-setup
 *   @@@ FILE &lt;sha256-hex&gt; &lt;relative/path&gt;
 *   |first line of the file
 *   |second line
 *   @@@ END
 * </pre>
 * Every content line starts with '|', so file contents can never be confused
 * with the markers. The SHA-256 is computed over the file text with '\n' line
 * endings, which catches any corruption or truncation of the download.
 *
 * <p>Safety rules:
 * <ul>
 *   <li>Everything is parsed and verified <b>before</b> anything is written.</li>
 *   <li>Paths must be relative, use '/', and may not contain '..'.</li>
 *   <li>A file that already exists with different content is only overwritten with --force.</li>
 *   <li>.cmd/.bat files are written with CRLF (Windows cmd needs it); everything else with LF.</li>
 * </ul>
 */
public final class Unbundle {

    private static final String HEADER = "# RAFTKV-BUNDLE v1";
    private static final String FILE_MARK = "@@@ FILE ";
    private static final String END_MARK = "@@@ END";
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    record Entry(String path, String expectedSha, String text) {}

    enum Status { NEW, SAME, CHANGED }

    public static void main(String[] args) throws Exception {
        String bundleArg = null;
        String targetArg = ".";
        boolean force = false;
        boolean dryRun = false;
        int positional = 0;
        for (String a : args) {
            switch (a) {
                case "--force" -> force = true;
                case "--dry-run" -> dryRun = true;
                default -> {
                    if (a.startsWith("--")) fail("unknown option " + a);
                    if (positional == 0) bundleArg = a;
                    else if (positional == 1) targetArg = a;
                    else fail("too many arguments");
                    positional++;
                }
            }
        }
        if (bundleArg == null) {
            System.err.println("usage: java Unbundle.java <bundle-file> [target-dir] [--force] [--dry-run]");
            System.exit(64);
        }

        Path bundle = Path.of(bundleArg);
        if (!Files.isRegularFile(bundle)) fail("bundle file not found: " + bundle.toAbsolutePath());
        Path target = Path.of(targetArg).toAbsolutePath().normalize();

        List<Entry> entries = parse(Files.readString(bundle, StandardCharsets.UTF_8));
        for (Entry e : entries) verifyHash(e);

        // Plan: decide what happens to every file before touching the disk.
        List<Status> plan = new ArrayList<>();
        List<String> conflicts = new ArrayList<>();
        for (Entry e : entries) {
            Path dest = resolveSafely(target, e.path());
            Status s;
            if (!Files.exists(dest)) {
                s = Status.NEW;
            } else if (Files.isDirectory(dest)) {
                fail("a directory exists where a file should go: " + dest);
                return;
            } else {
                String existing = Files.readString(dest, StandardCharsets.UTF_8).replace("\r\n", "\n");
                s = existing.equals(e.text()) ? Status.SAME : Status.CHANGED;
            }
            if (s == Status.CHANGED) conflicts.add(e.path());
            plan.add(s);
        }

        if (!conflicts.isEmpty() && !force) {
            System.err.println("These files already exist with different content:");
            conflicts.forEach(p -> System.err.println("  " + p));
            System.err.println("Nothing was written. Re-run with --force to overwrite them.");
            System.exit(2);
        }

        System.out.println((dryRun ? "[dry run] " : "") + "Unpacking " + entries.size() + " files into " + target);
        int written = 0;
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            Status s = plan.get(i);
            System.out.printf(Locale.ROOT, "  %-8s %s%n", s, e.path());
            if (s == Status.SAME || dryRun) continue;
            write(resolveSafely(target, e.path()), e);
            written++;
        }
        System.out.println(dryRun
                ? "Dry run finished; nothing written."
                : "Done: " + written + " written, " + (entries.size() - written) + " unchanged. All SHA-256 checks passed.");
    }

    static List<Entry> parse(String raw) {
        // Tolerate a UTF-8 BOM and Windows line endings added by editors or downloads.
        if (raw.startsWith("﻿")) raw = raw.substring(1);
        String[] lines = raw.replace("\r\n", "\n").split("\n", -1);
        if (lines.length == 0 || !lines[0].equals(HEADER)) {
            fail("not a RaftKV bundle (first line must be '" + HEADER + "')");
        }

        List<Entry> entries = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        int i = 1;
        while (i < lines.length) {
            String line = lines[i];
            if (line.isEmpty() || line.startsWith("#")) { i++; continue; }
            if (!line.startsWith(FILE_MARK)) fail("line " + (i + 1) + ": expected '" + FILE_MARK + "...'");

            String[] parts = line.substring(FILE_MARK.length()).split(" ", 2);
            if (parts.length != 2 || !SHA256_HEX.matcher(parts[0]).matches()) {
                fail("line " + (i + 1) + ": expected '" + FILE_MARK + "<sha256> <path>'");
            }
            String path = parts[1];
            validatePath(path, i + 1);
            if (!seen.add(path)) fail("line " + (i + 1) + ": duplicate file " + path);

            StringBuilder text = new StringBuilder();
            i++;
            boolean closed = false;
            while (i < lines.length) {
                String c = lines[i];
                if (c.equals(END_MARK)) { closed = true; i++; break; }
                if (!c.startsWith("|")) fail("line " + (i + 1) + ": content lines must start with '|' (in " + path + ")");
                text.append(c, 1, c.length()).append('\n');
                i++;
            }
            if (!closed) fail("file " + path + " has no '" + END_MARK + "' (bundle truncated?)");
            entries.add(new Entry(path, parts[0], text.toString()));
        }
        if (entries.isEmpty()) fail("bundle contains no files");
        return entries;
    }

    static void validatePath(String path, int lineNo) {
        boolean bad = path.isBlank()
                || path.startsWith("/")
                || path.contains("\\")
                || path.contains(":")
                || path.endsWith("/");
        if (!bad) {
            for (String seg : path.split("/", -1)) {
                if (seg.isEmpty() || seg.equals(".") || seg.equals("..")) { bad = true; break; }
            }
        }
        if (bad) fail("line " + lineNo + ": unsafe or invalid path '" + path + "'");
    }

    static Path resolveSafely(Path target, String relative) {
        Path p = target.resolve(relative).normalize();
        if (!p.startsWith(target)) fail("path escapes target folder: " + relative);
        return p;
    }

    static void verifyHash(Entry e) throws NoSuchAlgorithmException {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(e.text().getBytes(StandardCharsets.UTF_8));
        String actual = HexFormat.of().formatHex(digest);
        if (!actual.equals(e.expectedSha())) {
            fail("checksum mismatch for " + e.path() + " (download corrupted or edited). Nothing was written.");
        }
    }

    static void write(Path dest, Entry e) throws IOException {
        Files.createDirectories(dest.getParent());
        String lower = e.path().toLowerCase(Locale.ROOT);
        String text = (lower.endsWith(".cmd") || lower.endsWith(".bat")) ? e.text().replace("\n", "\r\n") : e.text();
        // Write to a temp file next to the destination, then move it into place.
        Path tmp = dest.resolveSibling(dest.getFileName() + ".unbundle-tmp");
        Files.writeString(tmp, text, StandardCharsets.UTF_8);
        Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
        if (e.path().equals("mvnw") || e.path().endsWith(".sh")) {
            dest.toFile().setExecutable(true, false); // no-op on Windows; matters on Linux/macOS
        }
    }

    static void fail(String message) {
        System.err.println("ERROR: " + message);
        System.exit(1);
    }

    private Unbundle() {}
}
