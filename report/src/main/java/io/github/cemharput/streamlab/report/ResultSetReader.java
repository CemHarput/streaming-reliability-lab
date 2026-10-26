package io.github.cemharput.streamlab.report;

import io.github.cemharput.streamlab.bench.Manifest;
import io.github.cemharput.streamlab.bench.ResultLayout;
import io.github.cemharput.streamlab.bench.json.Json;
import io.github.cemharput.streamlab.report.Checks.Finding;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Reads one result set, read-only. Everything below the root is located through {@link SafePaths}; file names come
 * from {@link ResultLayout}, never from the manifest.
 */
public final class ResultSetReader {

    public static final long MAX_MANIFEST_BYTES = 64L * 1024 * 1024;
    public static final long MAX_SUMMARY_BYTES = 1024L * 1024;

    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9._-]{1,120}");

    /** A file that is listed (name and size) but not analysed. */
    public record ListedFile(String name, long sizeBytes) {
    }

    /** What the convenience summary of the measured phase says. */
    public record SummaryData(Map<String, Long> outcomes, Long recordsWritten, Long requestsStarted) {
    }

    /** Raw facts of one run; {@code measured} is null when no usable measured file exists. */
    public record RunData(ManifestView.RunView run, List<Finding> findings, JsonlScanner.Result measured,
            SummaryData summary, SnapshotReader.Data pre, SnapshotReader.Data post, List<ListedFile> listed) {
    }

    public record ResultSet(ManifestView manifest, List<RunData> runs) {
    }

    private ResultSetReader() {
    }

    public static ResultSet read(Path root) throws InputException, IOException {
        Path realRoot = root.toRealPath();
        Optional<Path> manifestFile = SafePaths.file(realRoot, realRoot, ResultLayout.MANIFEST);
        if (manifestFile.isEmpty()) {
            throw new InputException("no manifest.json in the --results directory: pass the run-set directory"
                    + " (the directory that contains manifest.json, e.g. bench/results/<run-id>)");
        }
        if (Files.size(manifestFile.get()) > MAX_MANIFEST_BYTES) {
            throw new InputException("manifest.json is larger than " + MAX_MANIFEST_BYTES / (1024 * 1024) + " MiB");
        }
        Map<String, Object> parsed;
        try {
            parsed = Json.parseObject(Files.readString(manifestFile.get(), StandardCharsets.UTF_8));
        } catch (Json.JsonException ex) {
            throw new InputException("manifest.json is not a valid JSON object (" + ex.getMessage() + ")");
        } catch (java.nio.charset.CharacterCodingException ex) {
            throw new InputException("manifest.json is not valid UTF-8");
        }
        ManifestView manifest = ManifestView.of(parsed);
        List<RunData> runs = new ArrayList<>();
        for (ManifestView.RunView run : manifest.runs()) {
            runs.add(readRun(realRoot, run));
        }
        return new ResultSet(manifest, List.copyOf(runs));
    }

    private static RunData readRun(Path realRoot, ManifestView.RunView run) throws IOException {
        List<Finding> findings = new ArrayList<>();
        String status = run.status();
        boolean looksRun = Manifest.STATUS_COMPLETED.equals(status) || Manifest.STATUS_FAILED.equals(status)
                || Manifest.STATUS_INTERRUPTED.equals(status);
        if (!looksRun) {
            return new RunData(run, findings, null, null, null, null, List.of());
        }
        boolean expect = Manifest.STATUS_COMPLETED.equals(status);
        if (!SafePaths.validRunDir(run.dir(), run.id())) {
            findings.add(new Finding(Checks.RUN_DIR_INVALID, "the run directory is not of the form runs/<id>"));
            return new RunData(run, findings, null, null, null, null, List.of());
        }
        Optional<Path> dir = SafePaths.runDir(realRoot, run.id());
        if (dir.isEmpty()) {
            findings.add(new Finding(Checks.RUN_DIR_INVALID,
                    "the run directory is missing or leaves the result set"));
            return new RunData(run, findings, null, null, null, null, List.of());
        }
        Path runDir = dir.get();
        ManifestView.PhaseView measuredPhase = run.phase("measured");
        checkFileNames(measuredPhase, findings);

        JsonlScanner.Result measured = null;
        Optional<Path> jsonl = SafePaths.file(realRoot, runDir, ResultLayout.loadgenJsonl("measured"));
        if (jsonl.isPresent()) {
            try {
                measured = JsonlScanner.scan(jsonl.get());
            } catch (IOException ex) {
                findings.add(new Finding(Checks.MEASURED_JSONL_MISSING, "measured.jsonl could not be read"));
            }
        } else if (expect) {
            findings.add(new Finding(Checks.MEASURED_JSONL_MISSING, "measured.jsonl does not exist"));
        }
        if (measured != null) {
            if (measured.schemaUnsupported()) {
                findings.add(new Finding(Checks.JSONL_SCHEMA_UNSUPPORTED,
                        "the JSONL header schemaVersion is not 1; the file is not analysed"));
            } else {
                if (measured.malformedLines() > 0) {
                    findings.add(new Finding(Checks.JSONL_MALFORMED_LINE, measured.malformedLines()
                            + " line(s) skipped; first line numbers: " + joinNumbers(measured.firstMalformedLines())));
                }
                if (!measured.structureProblems().isEmpty()) {
                    findings.add(new Finding(Checks.JSONL_STRUCTURE, String.join("; ", measured.structureProblems())));
                }
            }
        }

        SummaryData summary = null;
        Optional<Path> summaryFile = SafePaths.file(realRoot, runDir, ResultLayout.loadgenSummary("measured"));
        if (summaryFile.isPresent()) {
            String unreadable = null;
            if (Files.size(summaryFile.get()) > MAX_SUMMARY_BYTES) {
                unreadable = "the summary is larger than 1 MiB";
            } else {
                try {
                    Map<String, Object> s = Json.parseObject(
                            Files.readString(summaryFile.get(), StandardCharsets.UTF_8));
                    Long schema = Jv.lng(s.get("schemaVersion"));
                    if (!"summary".equals(s.get("type"))) {
                        unreadable = "the summary file is not a summary record";
                    } else if (schema == null || schema != 1) {
                        findings.add(new Finding(Checks.SUMMARY_SCHEMA_UNSUPPORTED,
                                "the summary schemaVersion is not 1; the summary is not used"));
                    } else {
                        Map<String, Long> outcomes = new LinkedHashMap<>();
                        for (Map.Entry<String, Object> e : s.entrySet()) {
                            Long v = Jv.lng(e.getValue());
                            if (e.getKey().startsWith("outcome_") && v != null) {
                                outcomes.put(e.getKey().substring("outcome_".length()), v);
                            }
                        }
                        summary = new SummaryData(outcomes, Jv.lng(s.get("recordsWritten")),
                                Jv.lng(s.get("requestsStarted")));
                    }
                } catch (RuntimeException | java.nio.charset.CharacterCodingException ex) {
                    unreadable = "the summary could not be parsed";
                }
            }
            if (unreadable != null) {
                findings.add(new Finding(Checks.SUMMARY_UNREADABLE,
                        unreadable + "; the summary cross-checks are skipped"));
            }
        }

        SnapshotReader.Data pre = readSnapshot(realRoot, runDir, ResultLayout.SNAPSHOT_PRE, expect, findings);
        SnapshotReader.Data post = readSnapshot(realRoot, runDir, ResultLayout.SNAPSHOT_POST, expect, findings);
        return new RunData(run, findings, measured, summary, pre, post, listFiles(runDir));
    }

    private static void checkFileNames(ManifestView.PhaseView phase, List<Finding> findings) {
        Map<String, Object> files = phase.files();
        String[][] expected = {{"jsonl", ResultLayout.loadgenJsonl("measured")},
            {"summary", ResultLayout.loadgenSummary("measured")}};
        for (String[] e : expected) {
            Object v = files.get(e[0]);
            if (v != null && !e[1].equals(v)) {
                findings.add(new Finding(Checks.FILE_NAME_UNEXPECTED,
                        "the manifest names a different file for " + e[0] + "; the standard name is read"));
            }
        }
    }

    private static SnapshotReader.Data readSnapshot(Path realRoot, Path runDir, String name, boolean expect,
            List<Finding> findings) {
        Optional<Path> file = SafePaths.file(realRoot, runDir, name);
        if (file.isEmpty()) {
            if (expect) {
                findings.add(new Finding(Checks.SNAPSHOT_UNAVAILABLE, name + " does not exist"));
            }
            return null;
        }
        SnapshotReader.Data data = SnapshotReader.read(file.get());
        if (data == null || !data.gatewayListed()) {
            findings.add(new Finding(Checks.SNAPSHOT_UNAVAILABLE, name + " could not be read"));
            return null;
        }
        return data;
    }

    private static List<ListedFile> listFiles(Path runDir) {
        List<ListedFile> out = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(runDir)) {
            for (Path p : stream) {
                String name = p.getFileName().toString();
                if (!SAFE_NAME.matcher(name).matches() || !listedKind(name)) {
                    continue;
                }
                BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class,
                        LinkOption.NOFOLLOW_LINKS);
                if (attrs.isRegularFile()) {
                    out.add(new ListedFile(name, attrs.size()));
                }
            }
        } catch (IOException | RuntimeException ex) {
            return List.of();
        }
        out.sort(java.util.Comparator.comparing(ListedFile::name));
        return List.copyOf(out);
    }

    private static boolean listedKind(String name) {
        return name.equals(ResultLayout.loadgenJsonl("warmup")) || name.equals(ResultLayout.loadgenSummary("warmup"))
                || name.equals(ResultLayout.SNAPSHOT_READY) || name.contains("-gc.log") || name.endsWith(".log");
    }

    private static String joinNumbers(List<Long> numbers) {
        List<String> parts = new ArrayList<>();
        numbers.forEach(n -> parts.add(Long.toString(n)));
        return String.join(", ", parts);
    }
}
