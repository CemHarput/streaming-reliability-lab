package io.github.cemharput.streamlab.loadgen.sse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Incremental server-sent-events parser working on raw bytes. Lines end with LF, CR or CRLF (a CRLF split
 * across two reads is handled), a leading BOM is dropped, comment lines are ignored, and an event is
 * dispatched at a blank line when its {@code data} buffer is not empty. An event still open at {@link #end()}
 * is discarded. Not thread safe. UTF-8 decoding happens once per complete line, and CR/LF are ASCII, so a
 * multi-byte character split across reads is safe.
 */
public final class SseParser {

    public static final int DEFAULT_MAX_LINE_BYTES = 1024 * 1024;

    private final SseListener listener;
    private final int maxLineBytes;

    private byte[] line = new byte[256];
    private int lineLen;
    private boolean pendingCr;
    private boolean firstLine = true;

    private String eventType;
    private String eventId;
    private StringBuilder data;
    private final long maxEventDataBytes;

    public SseParser(SseListener listener) {
        this(listener, DEFAULT_MAX_LINE_BYTES);
    }

    public SseParser(SseListener listener, int maxLineBytes) {
        this.listener = listener;
        this.maxLineBytes = maxLineBytes;
        // One event may span many data lines; the total is capped too (16 lines of the maximum line size).
        this.maxEventDataBytes = 16L * maxLineBytes;
    }

    /** Consumes all remaining bytes of {@code buf}. Events completed by this call carry {@code nanos}. */
    public void feed(ByteBuffer buf, long nanos) {
        while (buf.hasRemaining()) {
            byte b = buf.get();
            if (pendingCr) {
                pendingCr = false;
                if (b == '\n') {
                    continue; // second half of CRLF; the CR already ended the line
                }
            }
            if (b == '\n') {
                endLine(nanos);
            } else if (b == '\r') {
                pendingCr = true;
                endLine(nanos);
            } else {
                if (lineLen >= maxLineBytes) {
                    throw new SseProtocolException("SSE line longer than " + maxLineBytes + " bytes");
                }
                if (lineLen == line.length) {
                    line = Arrays.copyOf(line, (int) Math.min((long) line.length * 2, maxLineBytes));
                }
                line[lineLen++] = b;
            }
        }
    }

    /** End of stream: an unfinished line and an undispatched event are dropped. */
    public void end() {
        lineLen = 0;
        pendingCr = false;
        eventType = null;
        eventId = null;
        data = null;
    }

    private void endLine(long nanos) {
        String text = new String(line, 0, lineLen, StandardCharsets.UTF_8);
        lineLen = 0;
        if (firstLine) {
            firstLine = false;
            if (!text.isEmpty() && text.charAt(0) == '\uFEFF') {
                text = text.substring(1);
            }
        }
        if (text.isEmpty()) {
            dispatch(nanos);
            return;
        }
        if (text.charAt(0) == ':') {
            return;
        }
        int colon = text.indexOf(':');
        String field;
        String value;
        if (colon < 0) {
            field = text;
            value = "";
        } else {
            field = text.substring(0, colon);
            value = text.substring(colon + 1);
            if (!value.isEmpty() && value.charAt(0) == ' ') {
                value = value.substring(1);
            }
        }
        switch (field) {
            case "event" -> eventType = value;
            case "id" -> {
                if (value.indexOf('\0') < 0) {
                    eventId = value;
                }
            }
            case "data" -> {
                if (data == null) {
                    data = new StringBuilder();
                } else {
                    data.append('\n');
                }
                if (data.length() + (long) value.length() > maxEventDataBytes) {
                    throw new SseProtocolException("SSE event data longer than " + maxEventDataBytes + " characters");
                }
                data.append(value);
            }
            default -> {
                // retry and unknown fields are ignored
            }
        }
    }

    private void dispatch(long nanos) {
        String type = eventType == null || eventType.isEmpty() ? "message" : eventType;
        String id = eventId;
        StringBuilder d = data;
        eventType = null;
        eventId = null;
        data = null;
        if (d == null) {
            return;
        }
        listener.onEvent(new SseEvent(type, id, d.toString()), nanos);
    }
}
