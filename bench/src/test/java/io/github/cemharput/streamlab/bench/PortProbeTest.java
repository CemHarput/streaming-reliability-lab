package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.testsupport.FakeTicker;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class PortProbeTest {

    @Test
    void connectProbeSeesARealListenerAndItsAbsence() throws Exception {
        int port;
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            port = server.getLocalPort();
            assertThat(PortProbe.connect().listenerPresent(port)).isTrue();
        }
        assertThat(PortProbe.connect().listenerPresent(port)).isFalse();
    }

    @Test
    void awaitNoListenerReturnsOnceTheListenerIsGone() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        PortProbe probe = port -> polls.incrementAndGet() < 5;
        FakeTicker ticker = new FakeTicker();
        assertThat(probe.awaitNoListener(List.of(1), Duration.ofSeconds(30), Duration.ofMillis(500), ticker)).isTrue();
        assertThat(ticker.sleepCount()).isEqualTo(4);
    }

    @Test
    void awaitNoListenerGivesUpAtTheTimeout() throws Exception {
        PortProbe probe = port -> true;
        FakeTicker ticker = new FakeTicker();
        long start = ticker.nanoTime();
        assertThat(probe.awaitNoListener(List.of(1, 2), Duration.ofSeconds(30), Duration.ofSeconds(1), ticker)).isFalse();
        assertThat(ticker.nanoTime() - start).isBetween(Duration.ofSeconds(30).toNanos(), Duration.ofSeconds(32).toNanos());
    }

    @Test
    void anyOfSeveralPortsKeepsTheWaitGoing() throws Exception {
        PortProbe probe = port -> port == 2;
        assertThat(probe.awaitNoListener(List.of(1, 2), Duration.ofSeconds(2), Duration.ofMillis(500), new FakeTicker()))
                .isFalse();
    }
}
