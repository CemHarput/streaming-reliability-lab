package io.github.cemharput.streamlab.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.cemharput.streamlab.simulator.SimulatorServer;
import io.github.cemharput.streamlab.testsupport.InProcessSimulatorConfig;
import io.github.cemharput.streamlab.testsupport.SseClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(InProcessSimulatorConfig.class)
@DirtiesContext
class ReadinessDownTest {

    @Value("${local.server.port}")
    int port;

    @Autowired
    SimulatorServer simulator;

    @Test
    void readinessGoesDownWhenSimulatorStops() throws Exception {
        String readiness = "http://localhost:" + port + "/actuator/health/readiness";
        String liveness = "http://localhost:" + port + "/actuator/health/liveness";

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(SseClient.get(readiness).statusCode()).isEqualTo(200));

        simulator.stop();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(SseClient.get(readiness).statusCode()).isEqualTo(503));
        assertThat(SseClient.get(liveness).statusCode()).isEqualTo(200);
    }
}
