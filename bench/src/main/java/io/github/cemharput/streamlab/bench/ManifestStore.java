package io.github.cemharput.streamlab.bench;

import io.github.cemharput.streamlab.bench.json.Json;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Owns the manifest map and writes it after every change: the JSON goes to a temporary file, which then replaces the
 * target with an {@code ATOMIC_MOVE} (retried a few times). If every attempt fails the previous file stays as it was
 * and the store is {@link #isDirty() dirty}. All access is serialised, so the shutdown hook and the runner thread
 * cannot interleave.
 */
public final class ManifestStore {

    private final Path file;
    private final Path tmp;
    private final Map<String, Object> root;
    private final Supplier<Instant> clock;
    private final Ticker ticker;
    private final Mover mover;
    private volatile Mover directMover = directDefault();
    private volatile TmpWriter directWriter = (f, c) -> Files.writeString(f, c, StandardCharsets.UTF_8);
    private boolean dirty;

    /** The final step of a write; replaceable in tests. */
    interface Mover {
        void move(Path from, Path to) throws IOException;
    }

    /** Moves are retried this many times: on Windows an antivirus or indexer can briefly lock the target. */
    static final int MOVE_ATTEMPTS = 5;
    private static final java.time.Duration MOVE_RETRY_PAUSE = java.time.Duration.ofMillis(100);

    public ManifestStore(Path file, Map<String, Object> root, Supplier<Instant> clock) {
        this(file, root, clock, Ticker.SYSTEM);
    }

    public ManifestStore(Path file, Map<String, Object> root, Supplier<Instant> clock, Ticker ticker) {
        this(file, root, clock, ticker, defaultMover());
    }

    /**
     * The move of the direct fallback: atomic, replacing the old file only when the new one is in place. Without
     * atomic move support a plain move is used only when the target does not exist, so the old file is never
     * deleted before the new content is in place.
     */
    static Mover directDefault() {
        return (from, to) -> {
            try {
                Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException ex) {
                if (Files.exists(to)) {
                    throw ex;
                }
                Files.move(from, to);
            }
        };
    }

    /** True when the last write failed, so the file may be older than the in-memory state. */
    public synchronized boolean isDirty() {
        return dirty;
    }

    /** The complete-state file written by {@link #writeDirect}; it stays in place when only the final move fails. */
    public Path directTmp() {
        return file.resolveSibling(file.getFileName() + ".direct.tmp");
    }

    static Mover defaultMover() {
        return (from, to) -> Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    ManifestStore(Path file, Map<String, Object> root, Supplier<Instant> clock, Ticker ticker, Mover mover) {
        this.ticker = ticker;
        this.mover = mover;
        this.file = file;
        this.tmp = file.resolveSibling(file.getFileName() + ".tmp");
        this.root = root;
        this.clock = clock;
    }

    /** Applies the change and writes the file. */
    public synchronized void update(Consumer<Map<String, Object>> change) {
        change.accept(root);
        writeLocked();
    }

    /** Like {@link #update} but a no-op once the manifest is no longer {@code running} (e.g. interrupted). */
    public synchronized void updateIfRunning(Consumer<Map<String, Object>> change) {
        if (Manifest.STATUS_RUNNING.equals(root.get("status"))) {
            update(change);
        }
    }

    /** Test seam: replaces the move of the direct fallback. */
    ManifestStore withDirectMover(Mover mover) {
        this.directMover = mover;
        return this;
    }

    /** Writes the serialised manifest to the direct tmp file; replaceable in tests. */
    interface TmpWriter {
        void write(Path file, String content) throws IOException;
    }

    /** Test seam: replaces the write of the direct tmp file. */
    ManifestStore withDirectWriter(TmpWriter writer) {
        this.directWriter = writer;
        return this;
    }

    /** What {@link #writeDirect} achieved. */
    public enum DirectResult {
        /** {@code manifest.json} now holds the current state. */
        WRITTEN,
        /** The move failed; {@code manifest.json} is untouched and the direct tmp file holds the complete state. */
        TMP_KEPT,
        /** Not even the direct tmp file could be written (a partial one is removed); there is no usable copy. */
        NOTHING
    }

    /**
     * Fallback when the normal write keeps failing: writes the complete in-memory manifest with the status it has
     * now. {@code finalizer} may adjust the root first (the runner uses it to turn a still {@code running} manifest
     * into {@code failed}); a manifest that is already completed, failed or interrupted keeps its status. The JSON
     * is serialised completely and written to a second temporary file ({@link #directTmp()}), which then replaces the
     * target with an atomic move. If anything fails the existing file is left untouched; the complete state
     * stays in the second temporary file only if the move failed (if the tmp file itself cannot be written, a
     * partial file is deleted and no copy remains, see {@link DirectResult}).
     *
     * @return whether the file holds the current state, only the direct tmp file does, or neither
     */
    public synchronized DirectResult writeDirect(Consumer<Map<String, Object>> finalizer) {
        Path direct = directTmp();
        try {
            finalizer.accept(root);
            directWriter.write(direct, Json.write(root));
        } catch (IOException | RuntimeException ex) {
            try {
                Files.deleteIfExists(direct); // never leave a partial copy behind
            } catch (IOException ignored) {
                // best effort
            }
            return DirectResult.NOTHING;
        }
        try {
            directMover.move(direct, file);
            dirty = false;
            return DirectResult.WRITTEN;
        } catch (IOException | RuntimeException ex) {
            return DirectResult.TMP_KEPT; // the old manifest.json is untouched, the complete direct tmp is kept
        }
    }

    public synchronized void write() {
        writeLocked();
    }

    /** A deep-enough copy for assertions: the serialised text. */
    public synchronized String json() {
        return Json.write(root);
    }

    public synchronized String status() {
        return (String) root.get("status");
    }

    private void writeLocked() {
        dirty = true;
        try {
            Files.writeString(tmp, Json.write(root), StandardCharsets.UTF_8);
            IOException last = null;
            for (int attempt = 1; attempt <= MOVE_ATTEMPTS; attempt++) {
                try {
                    mover.move(tmp, file);
                    dirty = false;
                    return;
                } catch (IOException ex) {
                    last = ex;
                    if (attempt < MOVE_ATTEMPTS) {
                        ticker.sleep(MOVE_RETRY_PAUSE);
                    }
                }
            }
            throw last;
        } catch (IOException ex) {
            throw new UncheckedIOException("cannot write " + file, ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException("interrupted while writing " + file, new IOException(ex));
        }
    }

    /**
     * Monotone: only a manifest that is still {@code running} becomes {@code interrupted}; a finished manifest
     * ({@code completed} or {@code failed}) is left alone. Running runs become interrupted, planned ones not_run.
     */
    @SuppressWarnings("unchecked")
    public synchronized void markInterrupted() {
        if (!Manifest.STATUS_RUNNING.equals(root.get("status"))) {
            return;
        }
        String now = clock.get().toString();
        root.put("status", Manifest.STATUS_INTERRUPTED);
        root.put("finishedAt", now);
        ((List<String>) root.get("flags")).add(Flags.INTERRUPTED);
        if (root.get("runs") instanceof List<?> runs) {
            for (Object o : runs) {
                Map<String, Object> run = (Map<String, Object>) o;
                if ("running".equals(run.get("status"))) {
                    run.put("status", Manifest.STATUS_INTERRUPTED);
                    run.put("endedAt", now);
                } else if (Manifest.RUN_PLANNED.equals(run.get("status"))) {
                    run.put("status", Manifest.RUN_NOT_RUN);
                }
            }
        }
        writeLocked();
    }
}
