package dev.raftkv.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import dev.raftkv.common.Bytes;
import dev.raftkv.common.GroupId;
import dev.raftkv.common.NodeId;
import dev.raftkv.protocol.ClientReply;
import dev.raftkv.protocol.ClientRequest;
import dev.raftkv.protocol.ClientStatus;
import dev.raftkv.protocol.Message;

/** The real TCP transport, over loopback. Every wait is bounded so a bug fails instead of hanging. */
class NioTransportTest {

    private static final NodeId A = NodeId.of(1);
    private static final NodeId B = NodeId.of(2);
    private static final GroupId G = GroupId.of(1);
    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);

    private static ClientRequest request(long seq) {
        return new ClientRequest(new UUID(1, 2), seq, 1, Bytes.ofUtf8("cmd-" + seq)); // seq >= 1
    }

    private static Envelope poll(BlockingQueue<Envelope> q) throws InterruptedException {
        Envelope e = q.poll(10, TimeUnit.SECONDS);
        if (e == null) {
            throw new AssertionError("no message within 10 s");
        }
        return e;
    }

    @Test
    void deliversInBothDirectionsWithAddressing() throws Exception {
        BlockingQueue<Envelope> atA = new LinkedBlockingQueue<>();
        BlockingQueue<Envelope> atB = new LinkedBlockingQueue<>();
        try (NioTransport a = NioTransport.start(A, ANY, Map.of(), atA::add);
             NioTransport b = NioTransport.start(B, ANY, Map.of(), atB::add)) {
            a.addPeer(B, b.localAddress());
            b.addPeer(A, a.localAddress());

            a.send(B, G, request(7));
            Envelope got = poll(atB);
            assertThat(got.from()).isEqualTo(A);
            assertThat(got.to()).isEqualTo(B);
            assertThat(got.group()).isEqualTo(G);
            assertThat(got.message()).isEqualTo(request(7));

            b.send(A, GroupId.of(42), request(8));
            Envelope back = poll(atA);
            assertThat(back.from()).isEqualTo(B);
            assertThat(back.group()).isEqualTo(GroupId.of(42));
            assertThat(back.message()).isEqualTo(request(8));
        }
    }

    @Test
    void keepsOrderOverManyMessagesAndLargeFrames() throws Exception {
        BlockingQueue<Envelope> atB = new LinkedBlockingQueue<>();
        try (NioTransport b = NioTransport.start(B, ANY, Map.of(), atB::add);
             NioTransport a = NioTransport.start(A, ANY, Map.of(B, b.localAddress()), e -> { })) {
            // Frames sent before the connection is up wait in a bounded queue (oldest dropped),
            // so connect first; from then on TCP plus one write queue must keep every frame in order.
            a.send(B, G, request(1));
            assertThat(poll(atB).message()).isEqualTo(request(1));
            int n = 20_000;
            byte[] big = new byte[512 * 1024];
            for (int i = 0; i < big.length; i++) {
                big[i] = (byte) (i * 31);
            }
            for (int i = 0; i < n; i++) {
                Bytes payload = i % 5_000 == 0 ? Bytes.copyOf(big) : Bytes.ofUtf8("m" + i);
                a.send(B, G, new ClientRequest(new UUID(0, 0), i + 1, 1, payload));
            }
            for (int i = 0; i < n; i++) {
                ClientRequest r = (ClientRequest) poll(atB).message();
                assertThat(r.sequence()).isEqualTo(i + 1L);
                if (i % 5_000 == 0) {
                    assertThat(r.command()).isEqualTo(Bytes.copyOf(big));
                }
            }
            assertThat(a.framesDropped()).isEqualTo(0L);
        }
    }

    @Test
    void aServerRepliesToAClientOverTheClientsOwnConnection() throws Exception {
        NodeId client = NodeId.of(5_000_000);
        BlockingQueue<Envelope> atClient = new LinkedBlockingQueue<>();
        NioTransport[] server = new NioTransport[1];
        try (NioTransport s = NioTransport.start(B, ANY, Map.of(), e -> {
                 ClientRequest r = (ClientRequest) e.message();
                 server[0].send(e.from(), e.group(), new ClientReply(r.sequence(), ClientStatus.OK, null, Bytes.ofUtf8("ok")));
             })) {
            server[0] = s;
            try (NioTransport c = NioTransport.client(client, Map.of(B, s.localAddress()), atClient::add)) {
                assertThat(c.localAddress() == null).isTrue();
                c.send(B, G, request(3));
                Envelope reply = poll(atClient);
                assertThat(reply.from()).isEqualTo(B);
                assertThat(((ClientReply) reply.message()).sequence()).isEqualTo(3L);
            }
        }
    }

    @Test
    void reconnectsWhenThePeerComesBackOnTheSamePort() throws Exception {
        BlockingQueue<Envelope> atB = new LinkedBlockingQueue<>();
        try (NioTransport a = NioTransport.start(A, ANY, Map.of(), e -> { })) {
            InetSocketAddress bAddress;
            try (NioTransport b = NioTransport.start(B, ANY, Map.of(), atB::add)) {
                bAddress = b.localAddress();
                a.addPeer(B, bAddress);
                a.send(B, G, request(1));
                assertThat(poll(atB).message()).isEqualTo(request(1));
            } // B goes down here

            // While B is down, sends are dropped or queued, never thrown.
            for (int i = 0; i < 20; i++) {
                a.send(B, G, request(100 + i));
                Thread.sleep(10);
            }

            BlockingQueue<Envelope> atB2 = new LinkedBlockingQueue<>();
            try (NioTransport b2 = NioTransport.start(B, bAddress, Map.of(), atB2::add)) {
                assertThat(b2.localAddress()).isEqualTo(bAddress);
                // Keep sending until a message gets through: A must notice and redial.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                Envelope got = null;
                while (got == null && System.nanoTime() < deadline) {
                    a.send(B, G, request(999));
                    got = atB2.poll(100, TimeUnit.MILLISECONDS);
                }
                if (got == null) {
                    throw new AssertionError("A never reconnected to the restarted B");
                }
                assertThat(got.from()).isEqualTo(A);
            }
        }
    }

    @Test
    void sendingToAnUnknownOrDeadPeerNeverThrows() throws Exception {
        try (NioTransport a = NioTransport.start(A, ANY, Map.of(B, new InetSocketAddress("127.0.0.1", 1)), e -> { })) {
            for (int i = 0; i < 100; i++) {
                a.send(B, G, request(i + 1));            // nobody listens on port 1
                a.send(NodeId.of(77), G, request(i + 1)); // no address at all
            }
            Thread.sleep(200);
            assertThat(a.framesSent()).isEqualTo(0L);
            assertThat(a.framesDropped()).isGreaterThanOrEqualTo(100L);
        }
    }

    @Test
    void aBadHandshakeGetsDisconnectedAndTheServerKeepsWorking() throws Exception {
        BlockingQueue<Envelope> atB = new LinkedBlockingQueue<>();
        try (NioTransport b = NioTransport.start(B, ANY, Map.of(), atB::add)) {
            try (Socket raw = new Socket()) {
                raw.connect(b.localAddress(), 5_000);
                raw.setSoTimeout(10_000);
                try (OutputStream out = raw.getOutputStream(); InputStream in = raw.getInputStream()) {
                    out.write("GET / HTTP/1.1\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                    out.flush();
                    byte[] buf = new byte[64];
                    int total = 0;
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        total += n; // B's own handshake may arrive before it hangs up
                    }
                    assertThat(total).isLessThanOrEqualTo(NioTransport.HANDSHAKE_BYTES);
                }
            }
            assertThat(atB.isEmpty()).isTrue();

            try (NioTransport a = NioTransport.start(A, ANY, Map.of(B, b.localAddress()), e -> { })) {
                a.send(B, G, request(1));
                Message m = poll(atB).message();
                assertThat(m).isEqualTo(request(1));
            }
        }
    }

    @Test
    void aPeerClaimingTheWrongIdentityIsRejected() throws Exception {
        BlockingQueue<Envelope> atImpostor = new LinkedBlockingQueue<>();
        // A believes B lives at this address, but node 3 answers there.
        try (NioTransport impostor = NioTransport.start(NodeId.of(3), ANY, Map.of(), atImpostor::add);
             NioTransport a = NioTransport.start(A, ANY, Map.of(B, impostor.localAddress()), e -> { })) {
            for (int i = 1; i <= 10; i++) {
                a.send(B, G, request(i));
                Thread.sleep(20);
            }
            Thread.sleep(300);
            assertThat(a.framesSent()).isEqualTo(0L);
            assertThat(atImpostor.isEmpty()).isTrue();
        }
    }
}
