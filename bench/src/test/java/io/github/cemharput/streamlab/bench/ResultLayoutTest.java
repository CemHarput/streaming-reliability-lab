package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.PlanBuilder.PlannedRun;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class ResultLayoutTest {

    @Test
    void pathsAndFixedFileNames() {
        Path root = Path.of("results", "id");
        ResultLayout layout = new ResultLayout(root);
        PlannedRun run = new PlannedRun(7, "007-c100-per-request-r2", 100, ConnectionMode.PER_REQUEST, 2, "BA");
        assertThat(layout.manifest()).isEqualTo(root.resolve("manifest.json"));
        assertThat(layout.manifestTmp()).isEqualTo(root.resolve("manifest.json.tmp"));
        assertThat(layout.runDir(run)).isEqualTo(root.resolve("runs").resolve("007-c100-per-request-r2"));
        assertThat(ResultLayout.relativeRunDir(run)).isEqualTo("runs/007-c100-per-request-r2");
        assertThat(ResultLayout.loadgenJsonl("warmup")).isEqualTo("warmup.jsonl");
        assertThat(ResultLayout.loadgenSummary("measured")).isEqualTo("measured-summary.json");
        assertThat(ResultLayout.loadgenLog("measured")).isEqualTo("measured-loadgen.log");
        assertThat(ResultLayout.loadgenGcLog("warmup")).isEqualTo("warmup-loadgen-gc.log");
        assertThat(ResultLayout.SNAPSHOT_READY).isEqualTo("snapshot-ready.json");
        assertThat(ResultLayout.SIMULATOR_LOG).isEqualTo("simulator.log");
        assertThat(ResultLayout.GATEWAY_GC_LOG).isEqualTo("gateway-gc.log");
    }
}
