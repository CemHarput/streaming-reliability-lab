package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class JdkProcessLauncherTest {

    @Test
    void removesSanitisedNamesCaseInsensitivelyAndAppliesOverridesAfterwards() {
        Map<String, String> env = new HashMap<>(Map.of("PATH", "p", "spring_profiles_active", "dev", "Lab_X", "1",
                "JAVA_TOOL_OPTIONS", "-Xfoo", "SIMULATOR_PORT", "1", "HOME", "h", "Logging_Level_Root", "debug"));
        var removed = ProcessSpecs.removedEnvironment(env);
        assertThat(removed).containsExactlyInAnyOrder("spring_profiles_active", "Lab_X", "JAVA_TOOL_OPTIONS",
                "SIMULATOR_PORT", "Logging_Level_Root");

        JdkProcessLauncher.applyEnvironment(env, removed, Map.of("SIMULATOR_PORT", "50051"));

        assertThat(env).containsOnlyKeys("PATH", "HOME", "SIMULATOR_PORT").containsEntry("SIMULATOR_PORT", "50051");
    }

    @Test
    void removalMatchesDifferentlyCasedNamesOfTheRecordedSet() {
        Map<String, String> env = new HashMap<>(Map.of("lab_x", "1", "PATH", "p"));
        JdkProcessLauncher.applyEnvironment(env, java.util.Set.of("LAB_X"), Map.of());
        assertThat(env).containsOnlyKeys("PATH");
    }
}
