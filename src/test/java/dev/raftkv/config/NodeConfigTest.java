package dev.raftkv.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.raftkv.common.NodeId;

class NodeConfigTest {

    private static final String PEERS = "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003";

    @Test
    void parsesAFullCommandLine() {
        NodeConfig c = NodeConfig.parse("--id", "2", "--peers", PEERS, "--data", "x/y", "--listen", "0.0.0.0:9000", "--verbose");
        assertThat(c.id()).isEqualTo(NodeId.of(2));
        assertThat(c.peers().size()).isEqualTo(3);
        assertThat(c.peers().get(NodeId.of(3)).getPort()).isEqualTo(7003);
        assertThat(c.listen().getPort()).isEqualTo(9000);
        assertThat(c.dataDir()).isEqualTo(Path.of("x/y"));
        assertThat(c.verbose()).isTrue();
    }

    @Test
    void defaultsListenAndDataFromTheNodesOwnEntry() {
        NodeConfig c = NodeConfig.parse("--id", "1", "--peers", PEERS);
        assertThat(c.listen()).isEqualTo(new InetSocketAddress("127.0.0.1", 7001));
        assertThat(c.dataDir()).isEqualTo(Path.of("data/n1"));
        assertThat(c.verbose()).isFalse();
    }

    @Test
    void peersComeOutInIdOrderWhateverTheInputOrder() {
        NodeConfig c = NodeConfig.parse("--id", "1", "--peers", "3=h:3, 1=h:1 ,2=h:2");
        assertThat(c.peers().keySet().toString()).isEqualTo("[n1, n2, n3]");
    }

    @Test
    void acceptsBracketedIpv6() {
        InetSocketAddress a = NodeConfig.parseAddress("[::1]:7001");
        assertThat(a.getPort()).isEqualTo(7001);
        assertThat(a.getAddress().isLoopbackAddress()).isTrue();
    }

    @Test
    void rejectsBadInputWithReadableMessages() {
        assertThatThrownBy(() -> NodeConfig.parse("--peers", PEERS)).hasMessageContaining("--id is required");
        assertThatThrownBy(() -> NodeConfig.parse("--id", "1")).hasMessageContaining("--peers is required");
        assertThatThrownBy(() -> NodeConfig.parse("--id", "4", "--peers", PEERS)).hasMessageContaining("must include this node");
        assertThatThrownBy(() -> NodeConfig.parse("--id", "x", "--peers", PEERS)).hasMessageContaining("must be a number");
        assertThatThrownBy(() -> NodeConfig.parse("--id", "1", "--peers", PEERS, "--bogus")).hasMessageContaining("unknown argument");
        assertThatThrownBy(() -> NodeConfig.parse("--id", "1", "--peers")).hasMessageContaining("needs a value");
        assertThatThrownBy(() -> NodeConfig.parse("--id", "1", "--id", "1", "--peers", PEERS)).hasMessageContaining("given twice");
        assertThatThrownBy(() -> NodeConfig.parsePeers("1=h:1,1=h:2")).hasMessageContaining("listed twice");
        assertThatThrownBy(() -> NodeConfig.parsePeers("1:h:1")).hasMessageContaining("id=host:port");
        assertThatThrownBy(() -> NodeConfig.parseAddress("host")).hasMessageContaining("host:port");
        assertThatThrownBy(() -> NodeConfig.parseAddress("host:70000")).hasMessageContaining("out of range");
    }
}
