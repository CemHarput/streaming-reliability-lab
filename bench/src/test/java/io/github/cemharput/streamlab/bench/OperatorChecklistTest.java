package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.bench.BenchOptions.ChecklistMode;
import io.github.cemharput.streamlab.bench.OperatorChecklist.Result;
import io.github.cemharput.streamlab.bench.OperatorChecklist.Terminal;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class OperatorChecklistTest {

    private static Terminal scripted(boolean interactive, String... lines) {
        Deque<String> queue = new ArrayDeque<>(java.util.Arrays.asList(lines));
        return new Terminal() {
            @Override
            public boolean interactive() {
                return interactive;
            }

            @Override
            public String readLine(String prompt) {
                return queue.poll();
            }
        };
    }

    private static final PrintStream OUT = new PrintStream(new ByteArrayOutputStream());

    @Test
    void yesModeRecordsYesWithFlagSourceWithoutAsking() throws Exception {
        Result r = OperatorChecklist.run(ChecklistMode.YES, scripted(false), OUT);
        assertThat(r.source()).isEqualTo("flag");
        assertThat(r.answers().keySet()).containsExactlyElementsOf(OperatorChecklist.KEYS);
        assertThat(r.answers().values()).hasSize(3).containsOnly("yes");
        assertThat(r.flags()).isEmpty();
    }

    @Test
    void skipModeRecordsUnansweredAndFlagsIt() throws Exception {
        Result r = OperatorChecklist.run(ChecklistMode.SKIP, scripted(false), OUT);
        assertThat(r.source()).isEqualTo("skipped");
        assertThat(r.answers().values()).containsOnly("unanswered");
        assertThat(r.flags()).containsExactly(Flags.CHECKLIST_NOT_CONFIRMED);
        assertThat(r.note()).contains("hides no data");
    }

    @Test
    void askModeRecordsOperatorAnswersAndANoFlag() throws Exception {
        Result r = OperatorChecklist.run(ChecklistMode.ASK, scripted(true, "y", "No", " yes "), OUT);
        assertThat(r.source()).isEqualTo("operator");
        assertThat(r.answers()).containsEntry("otherAppsClosed", "yes").containsEntry("onAcPower", "no")
                .containsEntry("samePowerPlan", "yes");
        assertThat(r.flags()).containsExactly(Flags.CHECKLIST_ANSWER_NO);
    }

    @Test
    void threeInvalidAnswersMakeItUnanswered() throws Exception {
        Result r = OperatorChecklist.run(ChecklistMode.ASK, scripted(true, "maybe", "what", "huh", "yes", "yes"), OUT);
        assertThat(r.answers()).containsEntry("otherAppsClosed", "unanswered").containsEntry("onAcPower", "yes");
        assertThat(r.flags()).containsExactly(Flags.CHECKLIST_NOT_CONFIRMED);
    }

    @Test
    void endOfInputIsUnanswered() throws Exception {
        Result r = OperatorChecklist.run(ChecklistMode.ASK, scripted(true), OUT);
        assertThat(r.answers().values()).containsOnly("unanswered");
    }

    @Test
    void askWithoutATerminalIsAUsageError() {
        assertThatThrownBy(() -> OperatorChecklist.run(ChecklistMode.ASK, scripted(false), OUT))
                .isInstanceOf(UsageException.class).hasMessageContaining("--checklist yes|skip");
    }
}
