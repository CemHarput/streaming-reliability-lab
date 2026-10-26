package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.Percentiles.Dist;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.HdrHistogram.Histogram;

/**
 * Streams one loadgen JSONL file (header, request lines, footer) in a single pass. Memory is O(COMPLETED requests)
 * (three {@code long[]}); no line is kept. A line that cannot be read is skipped and counted.
 */
public final class JsonlScanner {

    public static final String COMPLETED = "COMPLETED";
    private static final int MAX_OUTCOME_NAMES = 64;
    private static final int MAX_REPORTED_LINES = 5;
    private static final long NANOS_PER_MILLI = 1_000_000L;

    /** Everything the report needs from one file. */
    public record Result(
            Map<String, Object> header,
            Map<String, Object> footer,
            boolean schemaUnsupported,
            long requestLines,
            Map<String, Long> outcomes,
            long completed,
            Dist ttfb,
            Dist ttfc,
            Dist total,
            Long completedInWindow,
            Long windowStartMs,
            Long windowEndMs,
            long generatorDelayCount,
            Long generatorDelayP99,
            Long generatorDelayMax,
            long malformedLines,
            List<Long> firstMalformedLines,
            List<String> structureProblems) {
    }

    private JsonlScanner() {
    }

    public static Result scan(Path file) throws IOException {
        try (BoundedLineReader reader = new BoundedLineReader(file)) {
            return scan(reader);
        }
    }

    static Result scan(BoundedLineReader reader) throws IOException {
        Map<String, Object> header = null;
        Map<String, Object> footer = null;
        boolean schemaUnsupported = false;
        long requestLines = 0;
        Map<String, Long> outcomes = new LinkedHashMap<>();
        long completed = 0;
        long[] ttfb = new long[64];
        long[] ttfc = new long[64];
        long[] total = new long[64];
        int nb = 0;
        int nc = 0;
        int nt = 0;
        long missingTtfb = 0;
        long missingTtfc = 0;
        long inWindow = 0;
        Long windowStart = null;
        Long windowEnd = null;
        Histogram delay = new Histogram(3);
        long malformed = 0;
        List<Long> firstMalformed = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        boolean sawFirstContent = false;
        boolean headerNotFirst = false;
        boolean requestAfterFooter = false;
        int footers = 0;
        int headers = 0;

        BoundedLineReader.Line line;
        while ((line = reader.next()) != null) {
            if (line.tooLong()) {
                malformed++;
                note(firstMalformed, line.number());
                continue;
            }
            String text = line.text();
            if (text.isBlank()) {
                continue;
            }
            Map<String, Object> obj;
            try {
                obj = Json.parseObject(text);
            } catch (RuntimeException ex) {
                malformed++;
                note(firstMalformed, line.number());
                continue;
            }
            String type = Jv.str(obj.get("type"));
            if ("header".equals(type)) {
                headers++;
                if (sawFirstContent) {
                    headerNotFirst = true;
                }
                sawFirstContent = true;
                if (header == null) {
                    header = obj;
                    Long schema = Jv.lng(obj.get("schemaVersion"));
                    if (schema == null || schema != 1) {
                        schemaUnsupported = true;
                        break;
                    }
                    Long ramp = Jv.lng(obj.get("rampUpMs"));
                    Long dur = Jv.lng(obj.get("durationMs"));
                    if (ramp != null && dur != null && dur > ramp && ramp >= 0) {
                        windowStart = ramp;
                        windowEnd = dur;
                    }
                }
            } else if ("footer".equals(type)) {
                sawFirstContent = true;
                footers++;
                if (footer == null) {
                    footer = obj;
                }
            } else if ("request".equals(type)) {
                sawFirstContent = true;
                String outcome = Jv.str(obj.get("outcome"));
                Long start = Jv.lng(obj.get("startOffsetNanos"));
                Long scheduled = Jv.lng(obj.get("scheduledOffsetNanos"));
                Long totalNanos = Jv.lng(obj.get("totalNanos"));
                if (outcome == null || start == null || scheduled == null || totalNanos == null
                        || (!outcomes.containsKey(outcome) && outcomes.size() >= MAX_OUTCOME_NAMES)) {
                    malformed++;
                    note(firstMalformed, line.number());
                    continue;
                }
                if (footer != null) {
                    requestAfterFooter = true;
                }
                requestLines++;
                outcomes.merge(outcome, 1L, Long::sum);
                delay.recordValue(Math.max(0, start - scheduled));
                if (COMPLETED.equals(outcome)) {
                    completed++;
                    Long b = Jv.lng(obj.get("ttfbNanos"));
                    Long c = Jv.lng(obj.get("ttfcNanos"));
                    if (b == null) {
                        missingTtfb++;
                    } else {
                        if (nb == ttfb.length) {
                            ttfb = Arrays.copyOf(ttfb, nb * 2);
                        }
                        ttfb[nb++] = b;
                    }
                    if (c == null) {
                        missingTtfc++;
                    } else {
                        if (nc == ttfc.length) {
                            ttfc = Arrays.copyOf(ttfc, nc * 2);
                        }
                        ttfc[nc++] = c;
                    }
                    if (nt == total.length) {
                        total = Arrays.copyOf(total, nt * 2);
                    }
                    total[nt++] = totalNanos;
                    if (windowStart != null) {
                        long end = start + totalNanos;
                        if (end >= windowStart * NANOS_PER_MILLI && end < windowEnd * NANOS_PER_MILLI) {
                            inWindow++;
                        }
                    }
                }
            } else {
                malformed++;
                note(firstMalformed, line.number());
            }
        }

        if (!schemaUnsupported) {
            if (headers == 0) {
                problems.add("header missing");
            } else if (headers > 1) {
                problems.add("more than one header");
            } else if (headerNotFirst) {
                problems.add("header is not the first line");
            }
            if (footers == 0) {
                problems.add("footer missing");
            } else if (footers > 1) {
                problems.add("more than one footer");
            }
            if (requestAfterFooter) {
                problems.add("request line after the footer");
            }
        }
        Dist dTtfb = Percentiles.of(ttfb, nb, missingTtfb);
        Dist dTtfc = Percentiles.of(ttfc, nc, missingTtfc);
        Dist dTotal = Percentiles.of(total, nt, 0);
        long delayCount = delay.getTotalCount();
        return new Result(header, footer, schemaUnsupported, requestLines, outcomes, completed, dTtfb, dTtfc, dTotal,
                windowStart == null || schemaUnsupported ? null : inWindow, windowStart, windowEnd, delayCount,
                delayCount == 0 ? null : delay.getValueAtPercentile(99), delayCount == 0 ? null : delay.getMaxValue(),
                malformed, List.copyOf(firstMalformed), List.copyOf(problems));
    }

    private static void note(List<Long> first, long lineNumber) {
        if (first.size() < MAX_REPORTED_LINES) {
            first.add(lineNumber);
        }
    }
}
