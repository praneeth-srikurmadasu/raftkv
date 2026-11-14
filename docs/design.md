# RaftKV design notes

This document grows with every milestone. It records **what** we built, **why** we chose it over the alternatives, and every place where we deviated from `SPEC.md`.

## 1. Guiding principles

1. **JDK only in production code.** Test libraries (JUnit 5, AssertJ, jqwik, JMH) are allowed in test scope only. `pom.xml` enforces this by giving every dependency `<scope>test</scope>`.
2. **Determinism first.** Time, randomness, scheduling, network and disk are injected, so a whole cluster can run on one thread in tests and any failure can be replayed from its seed (SPEC §3.2).
3. **The paper is the source of truth.** Every Figure 2 rule is a commented line of code, e.g. `// Fig.2 AppendEntries rule 3`.

## 2. Build and tooling decisions

| Decision | Choice | Why |
|---|---|---|
| Build tool | Maven 3.9.11 via the Maven Wrapper | Declarative, the most common build tool in Java shops; the wrapper pins the version so every machine and CI build the same way. |
| Java version | 21 (LTS) | Records, sealed interfaces, pattern-matching `switch`, virtual threads are all final features in 21. |
| Version checks | `maven-enforcer-plugin` | Building with JDK 17 by accident fails immediately with a clear message instead of strange compile errors. |
| Pinned plugin versions | All plugins pinned in `pom.xml` | Maven's built-in defaults for old plugins change between Maven versions; pinning makes builds reproducible. |
| Base package | `dev.raftkv` | SPEC suggests `dev.<you>.raftkv`; a short neutral package keeps imports readable. |

## 3. Architecture

_Filled in from Milestone 1 onward._

## 4. Deviations from SPEC.md

_None so far._
