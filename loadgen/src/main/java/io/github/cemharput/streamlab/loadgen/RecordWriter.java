package io.github.cemharput.streamlab.loadgen;

import io.github.cemharput.streamlab.loadgen.client.StreamParams;
import io.github.cemharput.streamlab.loadgen.client.StreamResult;
import io.github.cemharput.streamlab.loadgen.json.JsonLine;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * Writes the JSONL file: a header, one line per finished request, a footer. A single platform thread drains an
 * unbounded queue, so slow disk never delays a worker (the price: memory grows if the disk cannot keep up).
 * It also feeds the {@link Summary}.
 */
public final class RecordWriter implements AutoCloseable {

    /** One finished request as handed to the writer. */
    public record RequestRecord(long index, int worker, StreamParams params, long scheduledOffsetNanos,
            StreamResult result) {
    }

    private static final Object POISON = new Object();

    private final BufferedWriter out;
    private final Summary summary;
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final Thread thread;
    private volatile IOException failure;
    private volatile Runnable failureListener;
    private volatile long written;
    private boolean closed;

    public RecordWriter(Path file, boolean overwrite, Summary summary) throws IOException {
        this.summary = summary;
        this.out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                overwrite
                        ? new StandardOpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE}
                        : new StandardOpenOption[] {StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE});
        this.thread = new Thread(this::drain, "loadgen-record-writer");
        this.thread.setDaemon(true);
    }

    /** Writes the header synchronously and starts the writer thread. */
    public void start(String headerLine) throws IOException {
        out.write(headerLine);
        out.newLine();
        out.flush();
        thread.start();
    }

    /** Called once from the writer thread when writing a record failed (the run uses it to stop early). */
    public void onFailure(Runnable listener) {
        this.failureListener = listener;
    }

    public void submit(RequestRecord record) {
        queue.add(record);
    }

    /** The first write error, or null. After a failure further records are dropped. */
    public IOException failure() {
        return failure;
    }

    public long recordsWritten() {
        return written;
    }

    /** Drains the queue, writes the footer and closes the file. Call once, after all workers are done. */
    public void finish(Supplier<String> footerLine) throws IOException {
        stopThread();
        if (failure != null) {
            throw failure;
        }
        out.write(footerLine.get());
        out.newLine();
        out.flush();
        close();
    }

    @Override
    public void close() throws IOException {
        stopThread();
        out.close(); // idempotent
    }

    private void stopThread() {
        if (closed) {
            return;
        }
        closed = true;
        queue.add(POISON);
        if (thread.getState() != Thread.State.NEW) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void drain() {
        try {
            while (true) {
                Object item = queue.take();
                if (item == POISON) {
                    return;
                }
                RequestRecord r = (RequestRecord) item;
                summary.record(r.result());
                if (failure != null) {
                    continue;
                }
                try {
                    out.write(requestLine(r));
                    out.newLine();
                    written++;
                    if (queue.isEmpty()) {
                        out.flush();
                    }
                } catch (IOException ex) {
                    failure = ex;
                    Runnable listener = failureListener;
                    if (listener != null) {
                        listener.run();
                    }
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    static String requestLine(RequestRecord rec) {
        StreamResult r = rec.result();
        StreamParams p = rec.params();
        return new JsonLine()
                .field("type", "request")
                .field("index", rec.index())
                .field("worker", rec.worker())
                .field("paramChunks", p.chunks())
                .field("paramIntervalMs", p.intervalMs())
                .field("paramFirstDelayMs", p.firstDelayMs())
                .field("paramFault", p.fault())
                .field("paramFaultAtSeq", p.faultAtSeq())
                .field("paramSeed", p.seed())
                .field("scheduledOffsetNanos", rec.scheduledOffsetNanos())
                .field("startOffsetNanos", r.startOffsetNanos())
                .field("endOffsetNanos", r.endOffsetNanos())
                .field("outcome", r.outcome().name())
                .field("status", r.status() == null ? null : Long.valueOf(r.status()))
                .field("retryAfter", r.retryAfter())
                .field("requestId", r.requestId())
                .field("ttfbNanos", r.ttfbNanos())
                .field("firstBodyByteNanos", r.firstBodyByteNanos())
                .field("ttfcNanos", r.ttfcNanos())
                .field("totalNanos", r.totalNanos())
                .field("chunks", r.chunks())
                .field("lastSeq", r.lastSeq())
                .field("interChunkMinNanos", r.interChunkMinNanos())
                .field("interChunkMaxNanos", r.interChunkMaxNanos())
                .field("terminal", r.terminal())
                .field("errorCode", r.errorCode())
                .field("errorMessage", r.errorMessage())
                .field("completeChunks", r.completeChunks())
                .field("protocolViolations", r.protocolViolations())
                .field("timeoutKind", r.timeoutKind())
                .field("errorPhase", r.errorPhase())
                .field("errorClass", r.errorClass())
                .field("errorDetail", r.errorDetail())
                .field("bodyEnd", r.bodyEnd())
                .toString();
    }

    static String headerLine(Options o, Instant startedAt, String toolVersion, String commandLine) {
        Runtime rt = Runtime.getRuntime();
        return new JsonLine()
                .field("type", "header")
                .field("schemaVersion", 1L)
                .field("tool", "streamlab-loadgen")
                .field("toolVersion", toolVersion)
                .field("startedAt", startedAt.toString())
                .field("target", o.target().toString())
                .field("loadModel", "closed")
                .field("concurrency", o.concurrency())
                .field("durationMs", o.duration() == null ? null : Long.valueOf(o.duration().toMillis()))
                .field("requests", o.requests())
                .field("rampUpMs", o.rampUp().toMillis())
                .field("chunks", o.chunks().toString())
                .field("intervalMs", o.intervalMs().toString())
                .field("firstDelayMs", o.firstDelayMs().toString())
                .field("faultAtSeq", o.faultAtSeq().toString())
                .field("fault", o.fault())
                .field("faultRate", o.faultRate())
                .field("seed", o.seed())
                .field("connectTimeoutMs", o.connectTimeout().toMillis())
                .field("idleTimeoutMs", o.idleTimeout().toMillis())
                .field("drainTimeoutMs", o.drainTimeout().toMillis())
                .field("rejectPauseMs", o.rejectPause().toMillis())
                .field("httpVersion", "HTTP/1.1")
                .field("javaVersion", System.getProperty("java.version"))
                .field("javaVendor", System.getProperty("java.vendor"))
                .field("javaVmName", System.getProperty("java.vm.name"))
                .field("osName", System.getProperty("os.name"))
                .field("osArch", System.getProperty("os.arch"))
                .field("osVersion", System.getProperty("os.version"))
                .field("availableProcessors", rt.availableProcessors())
                .field("maxHeapBytes", rt.maxMemory())
                .field("timeUnit", "ns")
                .field("clock", "System.nanoTime; offsets relative to run start; client and gateway are expected on"
                        + " one machine")
                .field("coordinatedOmission", "not corrected: closed model, latency from actual send, slower"
                        + " responses lower the offered load")
                .field("commandLine", commandLine)
                .toString();
    }

    static String footerLine(Instant endedAt, long elapsedNanos, String stopReason, long requestsStarted,
            long recordsWritten, long interrupted, Summary summary) {
        return new JsonLine()
                .field("type", "footer")
                .field("endedAt", endedAt.toString())
                .field("elapsedNanos", elapsedNanos)
                .field("stopReason", stopReason)
                .field("requestsStarted", requestsStarted)
                .field("recordsWritten", recordsWritten)
                .field("interrupted", interrupted)
                .field("interChunkCount", summary.interChunkCount())
                .field("interChunkHistogramUnit", "ns")
                .field("interChunkHistogram", summary.encodedInterChunk())
                .toString();
    }
}
