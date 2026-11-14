# RaftKV design notes

This document grows with every milestone. It records **what** we built, **why** we chose it over the alternatives, and every place where we deviated from `SPEC.md`.

## 1. Guiding principles

1. **JDK only in production code.** Test libraries (JUnit 5, AssertJ, jqwik, JMH) are allowed in test scope only. `pom.xml` enforces this by giving every dependency `<scope>test</scope>`.
2. **Determinism first.** Time, randomness, scheduling, network and disk are injected, so a whole cluster can run on one thread in tests and any failure can be replayed from its seed (SPEC §3.2).
3. **The paper is the source of truth.** Every Figure 2 rule is a commented line of code, e.g. `// Fig.2 AppendEntries rule 3`.
4. **Untrusted input is hostile.** Anything read from the network or disk is validated before use; decoding failures are always `CodecException`.

## 2. Build and tooling decisions

| Decision | Choice | Why |
|---|---|---|
| Build tool | Maven 3.9.11 via the Maven Wrapper | Declarative, the most common build tool in Java shops; the wrapper pins the version so every machine and CI build the same way. |
| Java version | 21 (LTS) | Records, sealed interfaces, pattern-matching `switch`, virtual threads are all final features in 21. |
| Version checks | `maven-enforcer-plugin` | Building with JDK 17 by accident fails immediately with a clear message instead of strange compile errors. |
| Pinned plugin versions | All plugins pinned in `pom.xml` | Maven's built-in defaults for old plugins change between Maven versions; pinning makes builds reproducible. |
| Base package | `dev.raftkv` | SPEC suggests `dev.<you>.raftkv`; a short neutral package keeps imports readable. |
| CI | GitHub Actions, `./mvnw -B verify` on Ubuntu + Temurin 21 | Every push is built and tested on a clean machine. |

## 3. Architecture (so far)

```
 protocol   sealed Message + MessageCodec      frames ⇄ messages
 codec      BinaryWriter/Reader, FrameCodec    bytes  ⇄ frames
 transport  Transport ── SimulatedNetwork      (NioTransport in M7)
 sim        Simulation                         virtual time, seeded event order, trace hash
 common     Clock, Scheduler, Bytes, ids, Hashing, EventLoopScheduler
```

### Key decisions (Milestone 1)

| Decision | Choice | Alternatives considered | Why |
|---|---|---|---|
| Raft communication style | One-way messages; replies are ordinary messages | Request/response RPC with futures | Raft already tolerates loss, duplication and reordering; one-way messages keep the core free of futures and timeouts and match etcd/raft. Correlation ids are reserved for client RPCs. |
| Byte payloads | Immutable `Bytes` value class | `byte[]` in records | Arrays use identity equality, which silently breaks record `equals` and round-trip tests; `Bytes` also guarantees immutability. |
| Node ids | `record NodeId(int)` | String names | 4 bytes on the wire, cheap comparisons; friendly names can be mapped in the CLI. |
| Frame integrity | CRC32C over the whole frame including the length field | No checksum (trust TCP) | TCP's 16-bit checksum misses real corruption; CRC32C is hardware-accelerated in the JDK. |
| Untrusted lengths | Checked against bytes present before allocating; frame payload capped (16 MiB default) | Trust the prefix | Prevents out-of-memory crashes from corrupt or hostile input. |
| Event ordering in simulation | `(time, seeded tie-breaker, sequence)` | Insertion order for ties | Seeded tie-breaking explores different interleavings of simultaneous events across seeds while staying reproducible per seed. |
| Simulated messages | Encoded and decoded on every hop | Pass objects directly | Exercises the production codec constantly and guarantees nodes never share mutable state. |

## 4. Deviations from SPEC.md

1. **Message package.** SPEC places `Message` in `codec/` and the RPC records in `raft/rpc/`. Java only allows a `sealed` interface to permit classes in *other* packages when the code is a named module (`module-info.java`); in an ordinary classpath project all permitted classes must share one package. Adding a module would complicate testing for no real benefit, so `Message`, all message records and `MessageCodec` live together in `dev.raftkv.protocol`. `codec/` keeps the generic byte- and frame-level code.
2. **Simulation location.** SPEC lists `Simulation` under the test tree but `SimulatedNetwork` under `src/main`. Since the network depends on the simulation, both live in `src/main` (`dev.raftkv.sim`, `dev.raftkv.transport.sim`). This also lets the benchmarks and demos reuse them.
3. **Correlation ids for Raft.** SPEC §5.2 describes request/response matching by correlation id. Raft messages don't use it (see "communication style" above); it remains in the frame for client RPCs.
