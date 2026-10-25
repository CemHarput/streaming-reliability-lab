package io.github.cemharput.streamlab.bench;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

/**
 * Detects listeners by connecting to {@code 127.0.0.1:port}. A bind test is not used: on Windows the
 * {@code SO_REUSEADDR} and TIME_WAIT semantics can report a free port as taken or a taken one as free.
 */
public interface PortProbe {

    boolean listenerPresent(int port);

    /** True when none of the ports has a listener within {@code timeout} (checked at {@code interval}). */
    default boolean awaitNoListener(List<Integer> ports, Duration timeout, Duration interval, Ticker ticker)
            throws InterruptedException {
        long deadline = ticker.nanoTime() + timeout.toNanos();
        while (true) {
            boolean any = false;
            for (int port : ports) {
                if (listenerPresent(port)) {
                    any = true;
                    break;
                }
            }
            if (!any) {
                return true;
            }
            if (ticker.nanoTime() >= deadline) {
                return false;
            }
            ticker.sleep(interval);
        }
    }

    /** Connect based probe with a 3 s timeout. */
    static PortProbe connect() {
        return port -> {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 3000);
                return true;
            } catch (IOException ex) {
                return false;
            }
        };
    }
}
