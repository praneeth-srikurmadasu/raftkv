# Milestone 7: A real cluster on a real network, explained

Until now every node lived inside the simulator: one thread, virtual time, a fake network. This milestone runs the **same Raft and KV code** as separate processes talking TCP, writing real files, using the wall clock. Nothing in `raft/` or `kv/` changed to make that happen; that is the payoff of injecting the clock, scheduler, transport and disk from Milestone 1.

```
src/main/java/dev/raftkv/
├── Main.java                   starts one server process
├── config/NodeConfig.java      --id, --peers, --data, --listen, --verbose
├── server/RaftHost.java        the groups of one process; routes messages to each group's thread
├── transport/NioTransport.java non-blocking TCP: one selector thread, handshake, framing, reconnect
├── transport/TransportException.java
└── client/cli/RaftKVCli.java   interactive and one-shot command-line client
scripts/
├── run-cluster.ps1 / .sh       start N nodes            ├── kill-node.ps1 / .sh   kill one (no clean shutdown)
├── start-node.ps1  / .sh       (re)start one node       ├── stop-cluster.ps1 / .sh
├── cli.ps1 / .sh               the client, pre-configured for the local cluster
└── common.ps1 / .sh            shared helpers
```

---

## 1. Try it (Windows)

```powershell
cd C:\Users\ajayk\Projects\raftkv
.\scripts\run-cluster.ps1 -Clean        # compiles, then opens 3 windows: n1, n2, n3
```
Watch the windows: within a second one node logs `starting election`, then `became leader with votes from [...]`, and the others log `following leader nX`.

```powershell
.\scripts\cli.ps1                       # interactive client
raftkv> put city Hyderabad
OK
raftkv> get city
Hyderabad
raftkv> cas city Hyderabad Vizag
OK
raftkv> leader
n2
raftkv> quit
```
The `raftkv>` prompt is the client itself: it only understands its own commands (`help` lists them), not PowerShell. `get` on a key nobody has `put` yet prints `(not found)`.

(Since Milestone 9 the cluster is sharded and `leader` prints one leader per group; see 09-multi-raft-and-controller.md.)

Now kill the leader (here n2) with no warning, like pulling its power cord:
```powershell
.\scripts\kill-node.ps1 -Id 2
.\scripts\cli.ps1 get city              # still Vizag: a new leader was elected in ~0.3 s
.\scripts\cli.ps1 put after kill        # writes work with 2 of 3 nodes
.\scripts\start-node.ps1 -Id 2          # n2 recovers its log from data\n2 and catches up
.\scripts\stop-cluster.ps1
```
If PowerShell refuses to run scripts ("running scripts is disabled on this system"), allow local scripts once for your user: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

Kill two of the three nodes and the client times out after 15 s: with no majority, Raft refuses to make progress rather than risk an inconsistent answer. Start one back and it works again.

## 2. Non-blocking I/O in one page

The naive server uses one thread per connection, blocked in `read()`. Fine for 3 nodes, but Milestone 9 puts many Raft groups and clients on each process. `java.nio` lets **one thread** watch every socket:

- every `SocketChannel` is **non-blocking**: `read()` returns whatever bytes are available now (maybe 0), `write()` writes what fits in the OS buffer (maybe only part);
- a `Selector` tells the thread which channels are ready: `OP_ACCEPT` (new connection), `OP_CONNECT` (our dial finished), `OP_READ` (bytes arrived), `OP_WRITE` (buffer has room again).

Two consequences shape the code:
- **TCP is a byte stream, not messages.** One `read()` may return half a frame or three and a half frames. `FrameDecoder` (Milestone 1) buffers bytes and emits whole frames, checking each frame's CRC.
- **Partial writes.** Each connection has a write queue. We register `OP_WRITE` interest only while the queue is non-empty, write as much as the socket takes, and keep the rest for the next `OP_WRITE`. (Registering `OP_WRITE` permanently would spin the CPU at 100%: a socket is almost always writable.)

Other threads never touch sockets. `send()` encodes the frame on the caller's thread, puts a small task on a lock-free queue, and calls `selector.wakeup()`.

## 3. Connections

**Handshake.** Each side's first 9 bytes are `["RKV1"][version 1][its NodeId]`. Anything else (a browser, a port scanner, a different version) is disconnected before a single frame is parsed. If we dialed n2's address and someone claiming to be n3 answers, we hang up too.

**Who is on the other end.** Connections are looked up by the remote node id, whichever side dialed. That is how a server replies to a client: the client connects and introduces itself with an id (≥ 2²⁴, randomly chosen), and the reply goes back over that same connection. Servers never need clients' addresses.

**One connection carries every group.** Each frame has a group id. `RaftHost` reads it and passes the message to that group.

**Reconnects.** If a connection breaks, queued frames for it are dropped (Raft retransmits on its own) and the next send redials. Failed dials back off exponentially with **full jitter**: wait a random time between 0 and min(2 s, 50 ms × 2ⁿ). Without the randomness, every node would retry a recovering peer at the same instants.

**Bounded memory.** While a dial is pending, at most 1,024 frames wait (oldest dropped); a connected peer that stops reading gets at most 64 MiB queued, then new frames are dropped. A slow or dead peer cannot run the process out of memory.

## 4. Threads in a server process

```
selector thread ──(group id)──► group 1 event loop: RaftNode + KVServer + log + state machine
                              └► group 2 event loop ... (Milestone 9)
```
The selector thread does network I/O only. Every Raft and KV decision happens on the group's own single thread (`EventLoopScheduler`), exactly like the simulator's one thread, so the Raft code still needs no locks. `RaftHost.onMessage` is the only hand-off.

**Fail-stop.** If an fsync fails, we no longer know what is on disk, so continuing could break the durability promise. The event loop's error handler in `Main` prints the error and halts the process. Restarting recovers from what *is* on disk.

## 5. Durability, for real this time

`RealDisk`'s `force()` is `FileChannel.force(false)` (fdatasync on Linux, `FlushFileBuffers` on Windows): the OS must put the data, and the metadata needed to read it back, on the device before returning. Everything Milestone 4 proved with `SimulatedDisk` now applies to actual files: an acknowledged write was fsynced on a majority, so `kill-node` (or a power cut of one machine) cannot lose it.

One Windows note: Java cannot fsync a *directory* on Windows (`RealDisk.syncDirectory` is a no-op there). NTFS journals metadata such as file creation and renames, which covers the cases the directory sync protects on Linux.

## 6. The tests

| Test | What it proves |
|---|---|
| `NioTransportTest` | messages arrive with correct addressing, both directions; 20,000 frames incl. 512 KB ones stay in order; replies reach a client over its own connection; reconnect after the peer restarts on the same port; dead or unknown peers never throw; bad handshakes and impostors are rejected |
| `RealClusterTest` | 3 nodes over loopback TCP with real files in one JVM: leader election, all operations, the leader stopping, a new leader, the old one restarting from disk and catching up |
| `RaftKVCliTest` | every CLI command against a real cluster |
| `ProcessClusterTest` | **the gate**: 3 separate `java dev.raftkv.Main` processes; the leader process is killed with `destroyForcibly` (kill -9), writes continue, the killed node restarts, the *new* leader is killed too (so the restarted node is needed for a majority), and every acknowledged write is still readable |
| `NodeConfigTest` | argument parsing and its error messages |

These tests use real time, so unlike the simulator they're not replayable from a seed. Every wait is bounded (nothing can hang the build), ports are picked free at run time, and each test runs against loopback only.

## 7. What is deliberately not here yet

See "Deviations" in `docs/design.md`: one listening port for peers and clients (SPEC suggests two), no correlation-id request futures in the transport (the client already matches replies by sequence number), and back-pressure drops frames instead of replying `OVERLOADED`. Snapshots (Milestone 8) come next: today a node that restarts replays its whole log, which grows forever.
