package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class BoundedLogPumpTest {

    @TempDir
    Path dir;

    @Test
    void copiesEverythingBelowTheLimit() throws Exception {
        Path file = dir.resolve("a.log");
        BoundedLogPump pump = new BoundedLogPump(new ByteArrayInputStream("hello\nworld\n".getBytes(StandardCharsets.UTF_8)),
                file, 1024, "t").start();
        assertThat(pump.await(Duration.ofSeconds(20))).isTrue();
        assertThat(Files.readString(file)).isEqualTo("hello\nworld\n");
        assertThat(pump.bytesWritten()).isEqualTo(12);
        assertThat(pump.bytesDropped()).isZero();
    }

    @Test
    void truncatesAtTheLimitCountsDroppedBytesAndAppendsANote() throws Exception {
        Path file = dir.resolve("b.log");
        byte[] data = "x".repeat(20_000).getBytes(StandardCharsets.UTF_8);
        BoundedLogPump pump = new BoundedLogPump(new ByteArrayInputStream(data), file, 10_000, "t").start();
        assertThat(pump.await(Duration.ofSeconds(20))).isTrue();
        assertThat(pump.bytesWritten()).isEqualTo(10_000);
        assertThat(pump.bytesDropped()).isEqualTo(10_000);
        String text = Files.readString(file);
        assertThat(text).startsWith("x".repeat(10_000)).contains("log truncated, 10000 bytes dropped");
    }

    @Test
    void aWriteErrorIsRecordedAndTheOutputIsStillDrained() throws Exception {
        PipedInputStream in = new PipedInputStream(4096);
        PipedOutputStream out = new PipedOutputStream(in);
        java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
        BoundedLogPump pump = new BoundedLogPump(in, dir.resolve("d.log"), 1_000_000, "t", file -> new java.io.OutputStream() {
            @Override
            public void write(int b) throws java.io.IOException {
                write(new byte[] {(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws java.io.IOException {
                if (writes.incrementAndGet() > 2) {
                    throw new java.io.IOException("No space left on device");
                }
            }
        }).start();
        byte[] block = new byte[1024];
        for (int i = 0; i < 64; i++) { // would block the writer if the pump stopped reading after the error
            out.write(block);
        }
        out.close();
        assertThat(pump.await(Duration.ofSeconds(20))).isTrue();
        assertThat(pump.writeError()).contains("No space left on device");
        // bytes lost to the error are not "dropped by the size limit" (that would raise LOG_TRUNCATED wrongly)
        assertThat(pump.bytesWritten() + pump.bytesLostAfterWriteError()).isEqualTo(64 * 1024);
        assertThat(pump.bytesLostAfterWriteError()).isPositive();
        assertThat(pump.bytesDropped()).isZero();
    }

    @Test
    void aFileThatCannotBeOpenedStillDrainsTheOutput() throws Exception {
        BoundedLogPump pump = new BoundedLogPump(new ByteArrayInputStream(new byte[5000]), dir.resolve("e.log"), 100, "t",
                file -> {
                    throw new java.io.IOException("denied");
                }).start();
        assertThat(pump.await(Duration.ofSeconds(20))).isTrue();
        assertThat(pump.writeError()).contains("denied");
        assertThat(pump.bytesWritten()).isZero();
        assertThat(pump.bytesDropped()).isZero();
        assertThat(pump.bytesLostAfterWriteError()).isEqualTo(5000);
    }

    @Test
    void healthyPumpHasNoWriteError() throws Exception {
        BoundedLogPump pump = new BoundedLogPump(new ByteArrayInputStream(new byte[10]), dir.resolve("f.log"), 100, "t").start();
        assertThat(pump.await(Duration.ofSeconds(20))).isTrue();
        assertThat(pump.writeError()).isNull();
    }

    @Test
    void keepsReadingAfterTheLimitSoTheWriterNeverBlocks() throws Exception {
        Path file = dir.resolve("c.log");
        PipedInputStream in = new PipedInputStream(4096);
        PipedOutputStream out = new PipedOutputStream(in);
        BoundedLogPump pump = new BoundedLogPump(in, file, 100, "t").start();
        byte[] block = new byte[1024];
        for (int i = 0; i < 64; i++) { // 64 KiB through a 4 KiB pipe: would block if the pump stopped reading
            out.write(block);
        }
        out.close();
        assertThat(pump.await(Duration.ofSeconds(20))).isTrue();
        assertThat(pump.bytesWritten()).isEqualTo(100);
        assertThat(pump.bytesDropped()).isEqualTo(64 * 1024 - 100);
    }
}
