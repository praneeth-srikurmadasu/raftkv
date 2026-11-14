# Milestone 0: Project setup explained

Goal of this step: a project that builds and tests the same way on your laptop and in CI, before we write any Raft code. Every file here is explained below.

## 1. The tools

| Tool | What it does | Why we need it |
|---|---|---|
| **JDK 21** (Eclipse Temurin) | Compiler (`javac`) + runtime (`java`) | Java 21 is the LTS release where records, sealed interfaces, pattern-matching `switch` and virtual threads are all final. |
| **Maven** | Downloads test libraries, compiles, runs tests, packages jars | Standard Java build tool; driven by `pom.xml`. |
| **Maven Wrapper** (`mvnw.cmd`, `mvnw`, `.mvn/wrapper/`) | Small scripts that download the exact Maven version on first use into `~/.m2/wrapper` | Nobody installs Maven by hand, and everyone (you, CI, a reviewer) gets Maven 3.9.11. |
| **Git** | Version control | Every milestone is a commit you can go back to. |
| **VS Code + Extension Pack for Java** | Editor, debugger, test runner | Reads `pom.xml`, so VS Code understands the project structure and dependencies. |

## 2. `pom.xml`, section by section

- **Coordinates** (`groupId`, `artifactId`, `version`): the project's identity. `0.1.0-SNAPSHOT` means "in development".
- **`maven.compiler.release=21`**: compiles against the Java 21 API exactly. `release` (unlike the older `source`/`target`) also stops you from accidentally calling APIs from newer JDKs.
- **`junit-bom`**: a "bill of materials" that pins all JUnit modules to one version so they can't drift apart.
- **Dependencies, all `<scope>test</scope>`**:
  - *JUnit 5*: the test runner and `@Test`.
  - *AssertJ*: readable assertions: `assertThat(x).isEqualTo(y)`.
  - *jqwik*: property-based testing. You state a property ("decode(encode(m)) == m") and jqwik checks it against hundreds of random inputs, then **shrinks** a failure to the smallest counterexample. We'll use it heavily for the codec in Milestone 1.
  - `test` scope means none of these can be imported from `src/main`, which enforces the "core Java only" rule at compile time.
- **Plugins**:
  - *enforcer*: fails the build if the JDK is older than 21 or Maven older than 3.9.
  - *compiler*: `-Xlint:all` shows every warning (we keep zero); `-parameters` keeps parameter names in class files.
  - *surefire*: runs tests during `mvnw test`. It finds both JUnit and jqwik tests, because both plug into the "JUnit Platform".
  - *pluginManagement*: pins versions of plugins Maven uses implicitly (clean, resources, jar…), so builds are reproducible.

## 3. Maven's standard layout

```
src/main/java   → production code, compiled into target/classes
src/test/java   → test code, compiled into target/test-classes (can see main, not vice versa)
target/         → all build output (ignored by Git; `mvnw clean` deletes it)
```

The package `dev.raftkv` means the files live in `src/main/java/dev/raftkv/`. Directory = package is a Java rule.

## 4. The smoke tests (`ToolchainTest.java`)

Each test checks one assumption the whole project depends on:

| Test | What it proves |
|---|---|
| `runsOnJava21OrNewer` | The JVM running the tests is really 21+. |
| `sealedTypesRecordsAndPatternSwitchWork` | The compiler accepts a sealed interface, records and an **exhaustive** `switch` with no `default`. Add a third `Shape` and the switch stops compiling. That's exactly how our `Message` codec will catch a forgotten message type. |
| `virtualThreadsWork` | `Thread.ofVirtual()` exists. We'll use virtual threads for blocking I/O in the real transport. |
| `crc32cMatchesKnownCheckValue` | `java.util.zip.CRC32C` gives the standard check value `0xE3069283` for `"123456789"`. Every frame and log record will carry a CRC32C. |
| `bannerMentionsVersion` | Main code is visible from test code. |
| `reversingTwiceGivesTheOriginalList` | jqwik is running (you'll see it report 1000 tries). |

## 5. Line endings: `.gitattributes`

Windows uses CRLF (`\r\n`), Linux uses LF (`\n`). Without rules, Git can flip them and break things:
- `mvnw` with CRLF fails on Linux CI (`/bin/sh^M: bad interpreter`).
- `mvnw.cmd` with LF can confuse `cmd.exe` labels.

So the repo stores LF for everything except `.cmd`/`.bat`, which stay CRLF.

**Executable bit:** Windows has no "executable" flag, so we tell Git directly that `mvnw` is executable (`git update-index --chmod=+x mvnw`). Without that, CI on Linux would fail with "Permission denied".

## 6. Check yourself

1. Why does every dependency in `pom.xml` have `test` scope? What would happen if `src/main` tried to `import org.assertj...`?
2. What's the difference between `maven.compiler.release=21` and just installing JDK 21?
3. Why use the Maven Wrapper instead of telling everyone to install Maven?
4. If you add `record Triangle(...) implements Shape` to the `permits` list, what breaks, and why is that a *good* thing?
5. Why does `mvnw` need LF line endings and the executable bit, even though you're on Windows?
