package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.BenchOptions.ChecklistMode;
import java.io.Console;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The operator checklist. Answers are recorded with their source. A "no" or a skipped checklist hides no data;
 * it only adds a flag to the manifest.
 */
public final class OperatorChecklist {

    public static final String OTHER_APPS_CLOSED = "otherAppsClosed";
    public static final String ON_AC_POWER = "onAcPower";
    public static final String SAME_POWER_PLAN = "samePowerPlan";
    public static final String NOTE = "Answers are recorded as given. A 'no', 'unanswered' or skipped checklist hides"
            + " no data; it only adds a flag.";

    private static final int MAX_INVALID = 3;
    private static final Map<String, String> QUESTIONS = Map.of(
            OTHER_APPS_CLOSED, "Are other applications (browsers, IDEs, sync clients) closed?",
            ON_AC_POWER, "Is the machine on AC power?",
            SAME_POWER_PLAN, "Is the same power plan active as in earlier run sets?");
    static final List<String> KEYS = List.of(OTHER_APPS_CLOSED, ON_AC_POWER, SAME_POWER_PLAN);

    /** Interactive input; tests supply a scripted one. */
    public interface Terminal {
        boolean interactive();

        /** Null at end of input. */
        String readLine(String prompt);

        static Terminal system() {
            Console console = System.console();
            return new Terminal() {
                @Override
                public boolean interactive() {
                    return console != null && console.isTerminal();
                }

                @Override
                public String readLine(String prompt) {
                    return console.readLine("%s", prompt);
                }
            };
        }
    }

    /** {@code source}: {@code flag}, {@code operator} or {@code skipped}. */
    public record Result(String source, Map<String, String> answers, String note, List<String> flags) {

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("source", source);
            m.put("answers", new LinkedHashMap<>(answers));
            m.put("note", note);
            return m;
        }
    }

    private OperatorChecklist() {
    }

    /** Fails before anything is created when {@code ask} has no terminal. */
    public static void requireTerminal(ChecklistMode mode, Terminal terminal) throws UsageException {
        if (mode == ChecklistMode.ASK && !terminal.interactive()) {
            throw new UsageException("--checklist ask needs an interactive terminal; use --checklist yes|skip");
        }
    }

    public static Result run(ChecklistMode mode, Terminal terminal, PrintStream out) throws UsageException {
        requireTerminal(mode, terminal);
        Map<String, String> answers = new LinkedHashMap<>();
        String source;
        switch (mode) {
            case YES -> {
                KEYS.forEach(k -> answers.put(k, "yes"));
                source = "flag";
            }
            case SKIP -> {
                KEYS.forEach(k -> answers.put(k, "unanswered"));
                source = "skipped";
            }
            default -> {
                source = "operator";
                for (String key : KEYS) {
                    answers.put(key, ask(key, terminal, out));
                }
            }
        }
        List<String> flags = new ArrayList<>();
        if (answers.containsValue("unanswered")) {
            flags.add(Flags.CHECKLIST_NOT_CONFIRMED);
        }
        if (answers.containsValue("no")) {
            flags.add(Flags.CHECKLIST_ANSWER_NO);
        }
        return new Result(source, answers, NOTE, flags);
    }

    private static String ask(String key, Terminal terminal, PrintStream out) {
        for (int attempt = 0; attempt < MAX_INVALID; attempt++) {
            String line = terminal.readLine(QUESTIONS.get(key) + " [yes/no] ");
            if (line == null) {
                return "unanswered";
            }
            switch (line.trim().toLowerCase(java.util.Locale.ROOT)) {
                case "y", "yes" -> {
                    return "yes";
                }
                case "n", "no" -> {
                    return "no";
                }
                default -> out.println("Please answer yes or no.");
            }
        }
        return "unanswered";
    }
}
