package dev.raftkv.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import dev.raftkv.codec.CodecException;
import dev.raftkv.codec.Frame;
import dev.raftkv.codec.FrameCodec;
import dev.raftkv.codec.FrameDecoder;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.Message;
import dev.raftkv.protocol.MessageCodec;

/**
 * The real network: non-blocking TCP with {@code java.nio} (SPEC §5.2).
 *
 * <h2>Threading</h2>
 * One selector thread owns every socket. {@link #send} may be called from any thread: it
 * encodes the frame on the caller's thread, hands it to the selector thread through a
 * lock-free queue, and wakes the selector. Incoming messages are delivered to the
 * {@link MessageHandler} <em>on the selector thread</em>; the handler must hand them to the right
 * event loop quickly (the node's {@code RaftHost} does exactly that).
 *
 * <h2>Connections</h2>
 * Each connection starts with a 9-byte handshake in both directions:
 * {@code ["RKV1":4][version:1][sender NodeId:4]}. After that it carries frames (SPEC §5.1).
 * Connections are found by the remote node id, whichever side dialed: a server can reply to a
 * client over the connection the client opened. One connection carries every Raft group
 * (frames carry the group id). A message to this node itself is handed to the handler directly.
 *
 * <h2>Failures</h2>
 * Delivery is best effort, as the {@link Transport} contract says. A broken connection is closed
 * and messages queued for it are dropped; the next send to that peer redials, with exponential
 * backoff and full jitter between attempts. While a dial is in progress, a bounded queue holds
 * outgoing frames; if it fills, the oldest are dropped (Raft retransmits anyway). A peer that
 * sends a bad handshake or a corrupt frame is disconnected.
 */
public final class NioTransport implements Transport, AutoCloseable {

    static final int MAGIC = 0x524B5631; // "RKV1"
    static final byte VERSION = 1;
    static final int HANDSHAKE_BYTES = 9;

    private static final int MAX_PENDING_FRAMES = 1024;
    private static final long MAX_QUEUED_BYTES = 64L * 1024 * 1024;
    private static final long BACKOFF_BASE_NANOS = 50_000_000L;     // 50 ms
    private static final long BACKOFF_MAX_NANOS = 2_000_000_000L;   // 2 s

    private final NodeId self;
    private final MessageHandler handler;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final Thread thread;
    private final Map<NodeId, InetSocketAddress> addresses = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();
    private volatile boolean closed;

    // ---- selector-thread state
    private final Map<NodeId, Connection> active = new HashMap<>();
    private final Map<NodeId, Connection> dialing = new HashMap<>();
    private final Map<NodeId, ArrayDeque<ByteBuffer>> waiting = new HashMap<>();
    private final Map<NodeId, Long> nextDialNanos = new HashMap<>();
    private final Map<NodeId, Integer> failures = new HashMap<>();

    // ---- counters (any thread)
    private final AtomicLong framesSent = new AtomicLong();
    private final AtomicLong framesReceived = new AtomicLong();
    private final AtomicLong framesDropped = new AtomicLong();

    private final class Connection {
        final SocketChannel channel; // opened (or accepted) by the constructor, so the connection owns it
        SelectionKey key;            // set right after registering with the selector
        final ByteBuffer readBuffer = ByteBuffer.allocate(64 * 1024);
        final ByteBuffer handshakeIn = ByteBuffer.allocate(HANDSHAKE_BYTES);
        final FrameDecoder decoder = new FrameDecoder();
        final ArrayDeque<ByteBuffer> writeQueue = new ArrayDeque<>();
        ByteBuffer handshakeOut;
        long queuedBytes;
        NodeId remote;          // known after the handshake (or the target, for dials)
        boolean handshakeDone;

        /** An outgoing connection to {@code target} (not yet connected). */
        Connection(NodeId target) throws IOException {
            this.channel = SocketChannel.open();
            this.remote = target;
        }

        /** The next incoming connection on {@code listener}; {@link #channel} is null if there was none. */
        Connection(ServerSocketChannel listener) throws IOException {
            this.channel = listener.accept();
        }
    }

    private NioTransport(NodeId self, InetSocketAddress listen, Map<NodeId, InetSocketAddress> peers,
                         MessageHandler handler) throws IOException {
        this.self = Objects.requireNonNull(self, "self");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.selector = Selector.open();
        if (listen != null) {
            server = ServerSocketChannel.open();
            NetworkChannel net = server;
            net.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            net.bind(listen);
            SelectableChannel selectable = server;
            selectable.configureBlocking(false);
            selectable.register(selector, SelectionKey.OP_ACCEPT);
        } else {
            server = null;
        }
        peers.forEach((id, addr) -> {
            if (!id.equals(self)) {
                addresses.put(id, addr);
            }
        });
        thread = new Thread(this::loop, "raftkv-nio-" + self);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * A transport that listens on {@code listen} (port 0 = any free port) and can dial {@code peers}.
     * {@code handler} receives every incoming message, on the selector thread.
     */
    public static NioTransport start(NodeId self, InetSocketAddress listen, Map<NodeId, InetSocketAddress> peers,
                                     MessageHandler handler) {
        try {
            return new NioTransport(self, listen, peers, handler);
        } catch (IOException e) {
            throw new TransportException("cannot start transport for " + self + " on " + listen, e);
        }
    }

    /** A transport that only dials out (for clients). */
    public static NioTransport client(NodeId self, Map<NodeId, InetSocketAddress> servers, MessageHandler handler) {
        return start(self, null, servers, handler);
    }

    // =====================================================================================
    // Public API (any thread)
    // =====================================================================================

    @Override
    public NodeId self() {
        return self;
    }

    @Override
    public void send(NodeId to, GroupId group, Message message) {
        if (closed) {
            return;
        }
        if (to.equals(self)) {
            // To ourselves (e.g. a data group's config poller asking the controller replica in the
            // same process): no socket, but still asynchronous, like any other delivery.
            commands.add(() -> handler.onMessage(new Envelope(self, self, group, message)));
            selector.wakeup();
            return;
        }
        ByteBuffer frame = FrameCodec.encode(MessageCodec.toFrame(group, 0, message));
        commands.add(() -> enqueue(to, frame));
        selector.wakeup();
    }

    /** Adds or changes the address of a peer (e.g. when its port is only known after it started). */
    public void addPeer(NodeId id, InetSocketAddress address) {
        addresses.put(id, address);
    }

    /** The address actually listened on (useful with port 0), or null for a client transport. */
    public InetSocketAddress localAddress() {
        try {
            return server == null ? null : (InetSocketAddress) server.getLocalAddress();
        } catch (IOException e) {
            throw new TransportException("cannot read local address", e);
        }
    }

    public long framesSent() {
        return framesSent.get();
    }

    public long framesReceived() {
        return framesReceived.get();
    }

    public long framesDropped() {
        return framesDropped.get();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        selector.wakeup();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // =====================================================================================
    // Selector thread
    // =====================================================================================

    private void loop() {
        try {
            while (!closed) {
                long timeoutMillis = untilNextRedialMillis();
                selector.select(timeoutMillis);
                Runnable r;
                while ((r = commands.poll()) != null) {
                    r.run();
                }
                redialDue();
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    handleKey(key);
                }
            }
        } catch (IOException | ClosedSelectorException e) {
            if (!closed) {
                throw new TransportException("transport " + self + " selector failed", e);
            }
        } finally {
            shutdown();
        }
    }

    private void handleKey(SelectionKey key) {
        if (!key.isValid()) {
            return;
        }
        if (key.isAcceptable()) {
            accept();
            return;
        }
        Connection c = (Connection) key.attachment();
        try {
            if (key.isConnectable()) {
                finishConnect(c);
            }
            if (key.isValid() && key.isReadable()) {
                read(c);
            }
            if (key.isValid() && key.isWritable()) {
                write(c);
            }
        } catch (IOException | CodecException e) {
            disconnect(c);
        }
    }

    private void accept() {
        Connection c;
        try {
            c = new Connection(server);
        } catch (IOException e) {
            return; // a failed accept affects only that one connection
        }
        if (c.channel == null) {
            return;
        }
        try {
            configure(c.channel);
            c.key = c.channel.register(selector, SelectionKey.OP_READ, c);
            queueHandshake(c);
        } catch (IOException e) {
            closeQuietly(c.channel);
        }
    }

    private void enqueue(NodeId to, ByteBuffer frame) {
        Connection c = active.get(to);
        if (c != null) {
            queueWrite(c, frame);
            return;
        }
        if (!addresses.containsKey(to)) {
            framesDropped.incrementAndGet(); // e.g. a client that has disconnected
            return;
        }
        ArrayDeque<ByteBuffer> q = waiting.computeIfAbsent(to, k -> new ArrayDeque<>());
        if (q.size() >= MAX_PENDING_FRAMES) {
            q.pollFirst();
            framesDropped.incrementAndGet();
        }
        q.addLast(frame);
        if (!dialing.containsKey(to) && System.nanoTime() >= nextDialNanos.getOrDefault(to, 0L)) {
            dial(to);
        }
    }

    private void dial(NodeId to) {
        InetSocketAddress addr = addresses.get(to);
        Connection c;
        try {
            c = new Connection(to);
        } catch (IOException e) {
            dialFailed(to);
            return;
        }
        try {
            configure(c.channel);
            boolean connected = c.channel.connect(addr);
            c.key = c.channel.register(selector, connected ? SelectionKey.OP_READ : SelectionKey.OP_CONNECT, c);
            dialing.put(to, c);
            if (connected) {
                queueHandshake(c);
            }
        } catch (IOException | RuntimeException e) {
            // RuntimeException: e.g. UnresolvedAddressException when the peer's host name didn't resolve
            closeQuietly(c.channel);
            dialFailed(to);
        }
    }

    private void finishConnect(Connection c) throws IOException {
        if (c.channel.finishConnect()) {
            c.key.interestOps(SelectionKey.OP_READ);
            queueHandshake(c);
        }
    }

    private void queueHandshake(Connection c) {
        ByteBuffer hs = ByteBuffer.allocate(HANDSHAKE_BYTES);
        hs.putInt(MAGIC).put(VERSION).putInt(self.value()).flip();
        c.handshakeOut = hs;
        queueWrite(c, hs);
    }

    private void read(Connection c) throws IOException {
        c.readBuffer.clear();
        int n = c.channel.read(c.readBuffer);
        if (n < 0) {
            disconnect(c);
            return;
        }
        c.readBuffer.flip();
        if (!c.handshakeDone) {
            while (c.handshakeIn.hasRemaining() && c.readBuffer.hasRemaining()) {
                c.handshakeIn.put(c.readBuffer.get());
            }
            if (c.handshakeIn.hasRemaining()) {
                return;
            }
            c.handshakeIn.flip();
            if (c.handshakeIn.getInt() != MAGIC || c.handshakeIn.get() != VERSION) {
                throw new CodecException("bad handshake");
            }
            int remoteId = c.handshakeIn.getInt();
            if (remoteId < 0) {
                throw new CodecException("bad node id in handshake");
            }
            NodeId remote = new NodeId(remoteId);
            if (c.remote != null && !c.remote.equals(remote)) {
                throw new CodecException("dialed " + c.remote + " but " + remote + " answered");
            }
            c.remote = remote;
            c.handshakeDone = true;
            established(c);
        }
        if (c.readBuffer.hasRemaining()) {
            List<Frame> frames = c.decoder.feed(c.readBuffer);
            for (Frame f : frames) {
                Message m = MessageCodec.fromFrame(f);
                framesReceived.incrementAndGet();
                handler.onMessage(new Envelope(c.remote, self, GroupId.of(f.groupId()), m));
            }
        }
    }

    /** Handshake complete: the connection becomes the route to its remote node. */
    private void established(Connection c) {
        if (dialing.get(c.remote) == c) {
            dialing.remove(c.remote);
        }
        active.put(c.remote, c); // the newest connection wins; an older one keeps receiving until it closes
        failures.remove(c.remote);
        nextDialNanos.remove(c.remote);
        ArrayDeque<ByteBuffer> q = waiting.remove(c.remote);
        if (q != null) {
            for (ByteBuffer frame : q) {
                queueWrite(c, frame);
            }
        }
    }

    private void queueWrite(Connection c, ByteBuffer buf) {
        if (c.queuedBytes + buf.remaining() > MAX_QUEUED_BYTES) {
            framesDropped.incrementAndGet(); // the peer isn't reading: shed load
            return;
        }
        c.writeQueue.addLast(buf);
        c.queuedBytes += buf.remaining();
        if (c.key.isValid() && (c.key.interestOps() & SelectionKey.OP_CONNECT) == 0) {
            c.key.interestOps(c.key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void write(Connection c) throws IOException {
        while (!c.writeQueue.isEmpty()) {
            ByteBuffer head = c.writeQueue.peekFirst();
            int before = head.remaining();
            c.channel.write(head);
            c.queuedBytes -= before - head.remaining();
            if (head.hasRemaining()) {
                return; // socket buffer full: wait for the next OP_WRITE
            }
            c.writeQueue.pollFirst();
            if (head != c.handshakeOut) {
                framesSent.incrementAndGet();
            }
        }
        c.key.interestOps(c.key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    private void disconnect(Connection c) {
        c.key.cancel();
        closeQuietly(c.channel);
        framesDropped.addAndGet(c.writeQueue.size());
        c.writeQueue.clear();
        if (c.remote != null) {
            if (active.get(c.remote) == c) {
                active.remove(c.remote);
            }
            if (dialing.get(c.remote) == c) {
                dialing.remove(c.remote);
                dialFailed(c.remote);
            }
        }
    }

    /** Exponential backoff with full jitter before the next attempt to reach {@code to}. */
    private void dialFailed(NodeId to) {
        int n = failures.merge(to, 1, Integer::sum);
        long cap = Math.min(BACKOFF_MAX_NANOS, BACKOFF_BASE_NANOS << Math.min(n, 10));
        nextDialNanos.put(to, System.nanoTime() + ThreadLocalRandom.current().nextLong(cap + 1));
    }

    private long untilNextRedialMillis() {
        long now = System.nanoTime();
        long best = Long.MAX_VALUE;
        for (Map.Entry<NodeId, ArrayDeque<ByteBuffer>> e : waiting.entrySet()) {
            if (!e.getValue().isEmpty() && !dialing.containsKey(e.getKey()) && !active.containsKey(e.getKey())) {
                best = Math.min(best, nextDialNanos.getOrDefault(e.getKey(), now) - now);
            }
        }
        if (best == Long.MAX_VALUE) {
            return 0; // select() with 0 = wait until woken up
        }
        return Math.max(1, best / 1_000_000 + 1);
    }

    private void redialDue() {
        long now = System.nanoTime();
        for (NodeId to : List.copyOf(waiting.keySet())) {
            ArrayDeque<ByteBuffer> q = waiting.get(to);
            if (q != null && !q.isEmpty() && !dialing.containsKey(to) && !active.containsKey(to)
                    && now >= nextDialNanos.getOrDefault(to, 0L)) {
                dial(to);
            }
        }
    }

    private static void configure(SocketChannel ch) throws IOException {
        // Called through the declaring interfaces, whose methods return the receiver's own type,
        // so IDE resource analysis doesn't mistake the fluent return values for new resources.
        SelectableChannel selectable = ch;
        selectable.configureBlocking(false);
        NetworkChannel net = ch;
        net.setOption(StandardSocketOptions.TCP_NODELAY, true); // Raft messages are small and latency-sensitive
        net.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
    }

    private static void closeQuietly(SocketChannel ch) {
        try {
            ch.close();
        } catch (IOException e) {
            // nothing more to do
        }
    }

    private void shutdown() {
        for (SelectionKey key : selector.keys()) {
            try {
                key.channel().close();
            } catch (IOException e) {
                // closing anyway
            }
        }
        try {
            selector.close();
        } catch (IOException e) {
            // closing anyway
        }
    }
}
