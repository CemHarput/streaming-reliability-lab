package io.github.cemharput.streamlab.loadgen.testsupport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Hand-written one-connection HTTP/1.1 server for failures the JDK HttpServer cannot produce: a TCP reset in
 * the middle of a chunked body, a chunked body cut off without its terminating chunk, a clean EOF without a
 * terminal event. Two chunk events are sent, then the server waits for {@link #release()} (the test opens it
 * once the client has seen both) and ends the connection in the chosen way.
 */
public final class RawSocketServer implements AutoCloseable {

    public enum Ending {
        /** SO_LINGER 0 + close: sends RST. */
        RESET,
        /** Plain close in the middle of a chunked body (FIN, no terminating zero chunk). */
        TRUNCATED_CHUNKED,
        /** {@code Connection: close} body without length, closed cleanly after the events. */
        CLEAN_EOF_NO_TERMINAL,
        /** A complete event follows the two chunks, then a plain close in the middle of the chunked body. */
        TRUNCATED_CHUNKED_AFTER_COMPLETE
    }

    private final ServerSocket socket;
    private final Ending ending;
    private final CountDownLatch release = new CountDownLatch(1);
    private final Thread thread;

    public RawSocketServer(Ending ending) throws IOException {
        this.ending = ending;
        this.socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        this.thread = new Thread(this::serve, "raw-socket-server");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    public URI uri() {
        return URI.create("http://127.0.0.1:" + socket.getLocalPort() + "/stream");
    }

    public void release() {
        release.countDown();
    }

    private void serve() {
        try (Socket client = socket.accept()) {
            readRequestHead(client.getInputStream());
            OutputStream out = client.getOutputStream();
            boolean chunked = ending != Ending.CLEAN_EOF_NO_TERMINAL;
            out.write(("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                    + (chunked ? "Transfer-Encoding: chunked\r\n" : "Connection: close\r\n") + "\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            for (int seq = 0; seq < 2; seq++) {
                String event = "event: chunk\nid: " + seq + "\ndata: {\"requestId\":\"r\",\"seq\":" + seq
                        + ",\"text\":\"t\",\"last\":false}\n\n";
                byte[] bytes = event.getBytes(StandardCharsets.UTF_8);
                if (chunked) {
                    out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                    out.write(bytes);
                    out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
                } else {
                    out.write(bytes);
                }
                out.flush();
            }
            if (ending == Ending.TRUNCATED_CHUNKED_AFTER_COMPLETE) {
                byte[] bytes = ("event: complete\ndata: {\"requestId\":\"r\",\"chunks\":2}\n\n")
                        .getBytes(StandardCharsets.UTF_8);
                out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                out.write(bytes);
                out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
            }
            if (!release.await(30, TimeUnit.SECONDS)) {
                return;
            }
            if (ending == Ending.RESET) {
                client.setSoLinger(true, 0);
            }
        } catch (IOException | InterruptedException ex) {
            // test is over or the client went away
        }
    }

    private static void readRequestHead(InputStream in) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) >= 0) {
            head.write(b);
            byte[] bytes = head.toByteArray();
            int n = bytes.length;
            if (n >= 4 && bytes[n - 4] == '\r' && bytes[n - 3] == '\n' && bytes[n - 2] == '\r' && bytes[n - 1] == '\n') {
                return;
            }
        }
    }

    @Override
    public void close() throws IOException {
        release.countDown();
        socket.close();
        thread.interrupt();
    }
}
