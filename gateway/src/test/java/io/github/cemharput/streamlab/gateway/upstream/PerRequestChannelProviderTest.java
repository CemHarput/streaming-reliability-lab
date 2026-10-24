package io.github.cemharput.streamlab.gateway.upstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.gateway.config.GatewayProperties;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.ConnectionMode;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Limits;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Retry;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Shutdown;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Timeouts;
import io.github.cemharput.streamlab.gateway.config.GatewayProperties.Upstream;
import io.github.cemharput.streamlab.testsupport.CountingChannelFactory;
import io.github.cemharput.streamlab.testsupport.CountingChannelFactory.CountingChannel;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The lease and provider of the per-request mode without a gateway: no server, channels point at an in-process name
 * nobody serves (creating such a channel does not connect). The races use a barrier and a spin offset and run a
 * bounded number of iterations; a stress test cannot prove the absence of a race, the argument is the lease CAS.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class PerRequestChannelProviderTest {

    private static final int ITERATIONS = 400;
    private static final int MAX_OFFSET_SPINS = 200;
    private static final Duration WAIT = Duration.ofSeconds(10);

    private final CountingChannelFactory factory = new CountingChannelFactory(
            target -> InProcessChannelBuilder.forName("per-request-test-nobody-listens").build());
    private final List<PerRequestChannelProvider> providers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        providers.forEach(PerRequestChannelProvider::stop);
        factory.channels().forEach(CountingChannel::shutdownNow);
    }

    private static GatewayProperties properties() {
        return new GatewayProperties(ConnectionMode.PER_REQUEST, new Upstream("localhost:1", Duration.ofSeconds(60)),
                new Shutdown(Duration.ofSeconds(10), Duration.ofSeconds(5)), Limits.defaults(), Timeouts.defaults(),
                Retry.defaults());
    }

    private PerRequestChannelProvider started() {
        PerRequestChannelProvider provider = new PerRequestChannelProvider(factory, properties());
        providers.add(provider);
        provider.start();
        return provider;
    }

    private static int closes(CountingChannel channel) {
        return channel.shutdownCalls() + channel.shutdownNowCalls();
    }

    // ---- single cases ----------------------------------------------------------------------------------------

    @Test
    void acquireFailsBeforeStartAndAfterStop() {
        PerRequestChannelProvider provider = new PerRequestChannelProvider(factory, properties());
        providers.add(provider);
        assertThatThrownBy(provider::acquire).isInstanceOf(IllegalStateException.class);
        assertThat(provider.state(false)).isEqualTo(ConnectivityState.SHUTDOWN);

        provider.start();
        assertThat(provider.isRunning()).isTrue();
        assertThat(provider.state(true)).isEqualTo(ConnectivityState.READY);
        provider.acquire().close();

        provider.stop();
        assertThat(provider.isRunning()).isFalse();
        assertThat(provider.state(false)).isEqualTo(ConnectivityState.SHUTDOWN);
        assertThatThrownBy(provider::acquire).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeWithoutChannelCreatesNothing() {
        PerRequestChannelProvider provider = started();
        ChannelProvider.Lease lease = provider.acquire();
        assertThat(factory.created()).isZero();
        lease.close();
        assertThat(factory.created()).isZero();
        assertThat(provider.openChannels()).isZero();
        assertThatThrownBy(lease::channel).isInstanceOf(IllegalStateException.class);
        assertThat(factory.created()).isZero();
    }

    @Test
    void sameLeaseReturnsTheSameChannelAndEachLeaseHasItsOwn() {
        PerRequestChannelProvider provider = started();
        ChannelProvider.Lease first = provider.acquire();
        ChannelProvider.Lease second = provider.acquire();
        Channel channel = first.channel();
        assertThat(first.channel()).isSameAs(channel);
        assertThat(second.channel()).isNotSameAs(channel);
        assertThat(factory.created()).isEqualTo(2);
        assertThat(provider.openChannels()).isEqualTo(2);
        first.close();
        second.close();
        assertThat(provider.openChannels()).isZero();
    }

    @Test
    void doubleCloseShutsTheChannelDownOnce() {
        PerRequestChannelProvider provider = started();
        ChannelProvider.Lease lease = provider.acquire();
        lease.channel();
        lease.close();
        lease.close();
        CountingChannel channel = factory.channels().get(0);
        assertThat(channel.shutdownCalls()).isEqualTo(1);
        assertThat(channel.shutdownNowCalls()).isZero();
        await().atMost(WAIT).until(channel::isTerminated);
    }

    @Test
    void channelAfterCloseThrowsAndCreatesNothing() {
        PerRequestChannelProvider provider = started();
        ChannelProvider.Lease lease = provider.acquire();
        lease.channel();
        lease.close();
        assertThatThrownBy(lease::channel).isInstanceOf(IllegalStateException.class);
        assertThat(factory.created()).isEqualTo(1);
    }

    @Test
    void factoryFailurePropagatesAndLeavesNothingOpen() {
        PerRequestChannelProvider provider = started();
        ChannelProvider.Lease lease = provider.acquire();
        factory.failNextCreations(1);
        assertThatThrownBy(lease::channel).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("injected channel creation failure");
        assertThat(provider.openChannels()).isZero();
        assertThat(factory.created()).isZero();
        // the lease is still usable: the next call creates the channel
        assertThat(lease.channel()).isNotNull();
        assertThat(factory.created()).isEqualTo(1);
        lease.close();
        lease.close();
        assertThat(closes(factory.channels().get(0))).isEqualTo(1);
    }

    @Test
    void stopClosesLeftoverLeaseOnceAndLaterCloseIsNoOp() {
        PerRequestChannelProvider provider = started();
        ChannelProvider.Lease lease = provider.acquire();
        lease.channel();
        assertThat(provider.openChannels()).isEqualTo(1);

        provider.stop();

        CountingChannel channel = factory.channels().get(0);
        assertThat(provider.openChannels()).isZero();
        assertThat(channel.isTerminated()).isTrue();
        assertThat(provider.isTerminated()).isTrue();
        lease.close();
        assertThat(channel.shutdownCalls()).isEqualTo(1);
        assertThat(channel.shutdownNowCalls()).isZero();
        assertThatThrownBy(lease::channel).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closedChannelsAreNotCountedAsOpen() {
        PerRequestChannelProvider provider = started();
        ChannelProvider.Lease lease = provider.acquire();
        lease.channel();
        lease.close();
        await().atMost(WAIT).until(() -> factory.channels().get(0).isTerminated());
        provider.acquire().channel();
        assertThat(factory.created()).isEqualTo(2);
        assertThat(provider.openChannels()).isEqualTo(1);
    }

    // ---- races -----------------------------------------------------------------------------------------------

    /** Runs the two sides at the same moment, one of them delayed by a swept number of spins. */
    private static void race(int iteration, Callable<Object> sideA, Callable<Object> sideB, ExecutorService executor,
            CyclicBarrier barrier) throws Exception {
        int offset = iteration % MAX_OFFSET_SPINS;
        boolean delayA = (iteration / MAX_OFFSET_SPINS) % 2 == 1;
        Future<Object> a = executor.submit(() -> {
            barrier.await(5, TimeUnit.SECONDS);
            if (delayA) {
                spin(offset);
            }
            return sideA.call();
        });
        Future<Object> b = executor.submit(() -> {
            barrier.await(5, TimeUnit.SECONDS);
            if (!delayA) {
                spin(offset);
            }
            return sideB.call();
        });
        a.get(5, TimeUnit.SECONDS);
        b.get(5, TimeUnit.SECONDS);
    }

    private static void spin(int spins) {
        for (int i = 0; i < spins; i++) {
            Thread.onSpinWait();
        }
    }

    private void assertEveryChannelClosedOnce(String what) {
        List<String> mismatches = new ArrayList<>();
        for (CountingChannel channel : factory.channels()) {
            if (closes(channel) != 1) {
                mismatches.add("shutdown=" + channel.shutdownCalls() + " shutdownNow=" + channel.shutdownNowCalls());
            }
        }
        assertThat(mismatches).as("%s: channels not closed exactly once", what).isEmpty();
    }

    @Test
    void closeRacingCloseClosesTheChannelOnce() throws Exception {
        PerRequestChannelProvider provider = started();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier barrier = new CyclicBarrier(2);
        try {
            for (int i = 0; i < ITERATIONS; i++) {
                ChannelProvider.Lease lease = provider.acquire();
                lease.channel();
                race(i, () -> {
                    lease.close();
                    return null;
                }, () -> {
                    lease.close();
                    return null;
                }, executor, barrier);
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(factory.created()).isEqualTo(ITERATIONS);
        assertEveryChannelClosedOnce("close vs close");
        assertThat(factory.channels()).allSatisfy(c -> assertThat(c.shutdownNowCalls()).isZero());
        assertThat(provider.openChannels()).isZero();
    }

    @Test
    void closeRacingChannelNeverLeavesAnOpenChannelBehind() throws Exception {
        PerRequestChannelProvider provider = started();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier barrier = new CyclicBarrier(2);
        try {
            for (int i = 0; i < ITERATIONS; i++) {
                ChannelProvider.Lease lease = provider.acquire();
                race(i, () -> {
                    try {
                        return lease.channel();
                    } catch (IllegalStateException expectedWhenCloseWins) {
                        return null;
                    }
                }, () -> {
                    lease.close();
                    return null;
                }, executor, barrier);
                // After close() returned, the lease never hands out an open channel again.
                assertThatThrownBy(lease::channel).isInstanceOf(IllegalStateException.class);
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        assertEveryChannelClosedOnce("close vs channel()");
        assertThat(provider.openChannels()).isZero();
    }

    @Test
    void stopRacingChannelClosesEveryCreatedChannelOnce() throws Exception {
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier barrier = new CyclicBarrier(2);
        List<PerRequestChannelProvider> used = new ArrayList<>();
        try {
            for (int i = 0; i < ITERATIONS; i++) {
                PerRequestChannelProvider provider = started();
                used.add(provider);
                ChannelProvider.Lease lease = provider.acquire();
                race(i, () -> {
                    try {
                        return lease.channel();
                    } catch (IllegalStateException expectedWhenStopWins) {
                        return null;
                    }
                }, () -> {
                    provider.stop();
                    return null;
                }, executor, barrier);
                assertThat(provider.openChannels()).isZero();
                assertThatThrownBy(lease::channel).isInstanceOf(IllegalStateException.class);
            }
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        assertEveryChannelClosedOnce("stop vs channel()");
        await().atMost(WAIT).until(() -> factory.channels().stream().allMatch(CountingChannel::isTerminated));
        assertThat(used).allSatisfy(p -> assertThat(p.openChannels()).isZero());
    }
}
