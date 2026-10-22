package io.github.cemharput.streamlab.simulator;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

public final class SimulatorMain {

    private static final Logger LOGGER = System.getLogger(SimulatorMain.class.getName());

    private SimulatorMain() {
    }

    public static void main(String[] args) throws Exception {
        SimulatorConfig config = SimulatorConfig.fromEnvironment(System.getenv());
        SimulatorServer server = SimulatorServer.start(
                Grpc.newServerBuilderForPort(config.port(), InsecureServerCredentials.create()),
                config
        );
        LOGGER.log(Level.INFO, "Token simulator listening on port {0}", String.valueOf(server.port()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, "simulator-shutdown"));
        server.awaitTermination();
    }
}
