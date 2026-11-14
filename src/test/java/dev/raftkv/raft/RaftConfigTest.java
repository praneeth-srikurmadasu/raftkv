package dev.raftkv.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Set;

import org.junit.jupiter.api.Test;

class RaftConfigTest {

    @Test
    void defaultsFollowThePaper() {
        RaftConfig c = RaftConfig.defaults();
        assertThat(c.electionTimeoutMin()).isEqualTo(Duration.ofMillis(150));
        assertThat(c.electionTimeoutMax()).isEqualTo(Duration.ofMillis(300));
        assertThat(c.heartbeatInterval()).isEqualTo(Duration.ofMillis(50));
        assertThat(c.bugs()).isEmpty();
    }

    @Test
    void heartbeatMustBeShorterThanTheElectionTimeout() {
        assertThatThrownBy(() -> new RaftConfig(Duration.ofMillis(150), Duration.ofMillis(300), Duration.ofMillis(150), Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RaftConfig(Duration.ofMillis(300), Duration.ofMillis(150), Duration.ofMillis(50), Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bugsAreOptIn() {
        RaftConfig c = RaftConfig.defaults().withBugs(Bug.VOTE_TWICE_IN_TERM);
        assertThat(c.has(Bug.VOTE_TWICE_IN_TERM)).isTrue();
        assertThat(c.has(Bug.VOTE_WITHOUT_LOG_CHECK)).isFalse();
        assertThat(RaftConfig.defaults().has(Bug.VOTE_TWICE_IN_TERM)).isFalse();
    }
}
