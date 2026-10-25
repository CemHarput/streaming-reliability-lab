package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cemharput.streamlab.bench.PlanBuilder.PlannedRun;
import io.github.cemharput.streamlab.bench.PlanBuilder.RunPlan;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class PlanBuilderTest {

    private static final List<ConnectionMode> AB = List.of(ConnectionMode.SHARED, ConnectionMode.PER_REQUEST);

    private static String letters(RunPlan plan, List<ConnectionMode> order) {
        return plan.runs().stream().map(r -> r.mode() == order.get(0) ? "A" : "B").collect(Collectors.joining());
    }

    @Test
    void alternatesAbbaForOneToFourRepetitions() {
        assertThat(letters(PlanBuilder.build(List.of(10), 1, AB), AB)).isEqualTo("AB");
        assertThat(letters(PlanBuilder.build(List.of(10), 2, AB), AB)).isEqualTo("ABBA");
        assertThat(letters(PlanBuilder.build(List.of(10), 3, AB), AB)).isEqualTo("ABBAAB");
        assertThat(letters(PlanBuilder.build(List.of(10), 4, AB), AB)).isEqualTo("ABBAABBA");
    }

    @Test
    void reversedModeOrderSwapsTheModes() {
        List<ConnectionMode> reversed = List.of(ConnectionMode.PER_REQUEST, ConnectionMode.SHARED);
        RunPlan plan = PlanBuilder.build(List.of(10), 2, reversed);
        assertThat(plan.runs().stream().map(PlannedRun::mode)).containsExactly(ConnectionMode.PER_REQUEST,
                ConnectionMode.SHARED, ConnectionMode.SHARED, ConnectionMode.PER_REQUEST);
        assertThat(letters(plan, reversed)).isEqualTo("ABBA");
    }

    @Test
    void concurrencyIsTheOuterLoopAndEachModeAppearsEquallyOften() {
        RunPlan plan = PlanBuilder.build(List.of(50, 100, 150), 4, AB);
        assertThat(plan.runs()).hasSize(24);
        assertThat(plan.runs().stream().map(PlannedRun::concurrency).distinct()).containsExactly(50, 100, 150);
        assertThat(plan.runs().subList(0, 8)).allMatch(r -> r.concurrency() == 50);
        for (ConnectionMode mode : ConnectionMode.values()) {
            assertThat(plan.runs().stream().filter(r -> r.mode() == mode && r.concurrency() == 100)).hasSize(4);
        }
    }

    @Test
    void idsAreUniqueOrderedAndNamedAfterTheCell() {
        RunPlan plan = PlanBuilder.build(List.of(100), 3, AB);
        assertThat(new HashSet<>(plan.runs().stream().map(PlannedRun::id).toList())).hasSize(6);
        assertThat(plan.runs().get(0).id()).isEqualTo("001-c100-shared-r1");
        assertThat(plan.runs().get(3).id()).isEqualTo("004-c100-shared-r2");
        assertThat(plan.runs().get(2).id()).isEqualTo("003-c100-per-request-r2");
        assertThat(plan.runs().stream().map(PlannedRun::id)).isSorted();
        assertThat(plan.runs().stream().map(PlannedRun::orderLabel)).containsExactly("AB", "AB", "BA", "BA", "AB",
                "AB");
    }
}
