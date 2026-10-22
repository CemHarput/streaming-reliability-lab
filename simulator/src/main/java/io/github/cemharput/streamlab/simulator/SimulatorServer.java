package io.github.cemharput.streamlab.simulator;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Owns the gRPC server, the emit scheduler and the health status.
 *
 * <p>Shutdown order: report NOT_SERVING, stop accepting calls, let in-flight
 * streams finish within the grace period, then cancel the rest and stop the
 * scheduler. Generations observe the cancel and release their state.
 */
public final class SimulatorServer implements AutoCloseable {

    private static final Logger LOGGER = System.getLogger(SimulatorServer.class.getName());
    private static final Duration FORCED_TERMINATION_WAIT = Duration.ofSeconds(5);

    private final Server server;
    private final ScheduledExecutorService scheduler;
    private final HealthStatusManager health;
    private final Duration shutdownGrace;
    private final AtomicBoolean stopping = new AtomicBoolean();

    private SimulatorServer(Server server, ScheduledExecutorService scheduler, HealthStatusManager health, Duration shutdownGrace) {
        this.server = server;
        this.scheduler = scheduler;
        this.health = health;
        this.shutdownGrace = shutdownGrace;
    }

    /**
     * Builds and starts the server on the given transport. Production uses a
     * Netty builder; tests pass an in-process builder.
     */
    public static SimulatorServer start(ServerBuilder<?> builder, SimulatorConfig config) throws IOException {
        SimulatorMetrics metrics = new SimulatorMetrics();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
                config.schedulerThreads(), daemonThreads("simulator-emit-"));
        HealthStatusManager health = new HealthStatusManager();

        Server server = builder
                .addService(new TokenStreamServiceImpl(scheduler, metrics, config.limits()))
                .addService(new SimulatorAdminServiceImpl(metrics))
                .addService(health.getHealthService())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build();
        try {
            server.start();
        } catch (IOException | RuntimeException ex) {
            scheduler.shutdownNow();
            throw ex;
        }
        health.setStatus("", ServingStatus.SERVING);
        return new SimulatorServer(server, scheduler, health, config.shutdownGrace());
    }

    public int port() {
        return server.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /** Graceful stop; safe to call more than once. */
    public void stop() throws InterruptedException {
        if (!stopping.compareAndSet(false, true)) {
            server.awaitTermination();
            return;
        }
        health.enterTerminalState();
        server.shutdown();
        if (!server.awaitTermination(shutdownGrace.toMillis(), TimeUnit.MILLISECONDS)) {
            LOGGER.log(Level.WARNING, "In-flight streams did not finish within {0}; cancelling", shutdownGrace);
            server.shutdownNow();
            if (!server.awaitTermination(FORCED_TERMINATION_WAIT.toMillis(), TimeUnit.MILLISECONDS)) {
                LOGGER.log(Level.ERROR, "Server did not terminate after forced shutdown");
            }
        }
        scheduler.shutdownNow();
    }

    @Override
    public void close() throws InterruptedException {
        stop();
    }

    private static ThreadFactory daemonThreads(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
