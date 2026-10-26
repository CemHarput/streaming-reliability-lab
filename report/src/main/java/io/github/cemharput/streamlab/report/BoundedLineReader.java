package io.github.cemharput.streamlab.report;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads UTF-8 lines with a hard upper bound per line. {@code \n}, {@code \r\n} and a lone {@code \r} end a line. A
 * line longer than the bound is skipped without being held in memory and reported as too long. Malformed UTF-8 is
 * replaced, never an error.
 */
public final class BoundedLineReader implements AutoCloseable {

    public static final int MAX_LINE_CHARS = 1024 * 1024;

    /** {@code text} is null when the line was longer than the bound. */
    public record Line(long number, String text) {

        public boolean tooLong() {
            return text == null;
        }
    }

    private final Reader reader;
    private final int maxChars;
    private final char[] buffer = new char[8192];
    private int pos;
    private int limit;
    private long lineNumber;
    private boolean pendingLf; // a \r ended the previous line: swallow a directly following \n

    public BoundedLineReader(Path file) throws IOException {
        this(Files.newInputStream(file), MAX_LINE_CHARS);
    }

    BoundedLineReader(InputStream in, int maxChars) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        this.reader = new InputStreamReader(in, decoder);
        this.maxChars = maxChars;
    }

    /** The next line, or null at the end of the input. */
    public Line next() throws IOException {
        StringBuilder sb = null;
        int length = 0;
        boolean tooLong = false;
        boolean any = false;
        while (true) {
            if (pos >= limit) {
                limit = reader.read(buffer, 0, buffer.length);
                pos = 0;
                if (limit < 0) {
                    limit = 0;
                    if (!any) {
                        return null;
                    }
                    lineNumber++;
                    return new Line(lineNumber, tooLong ? null : sb == null ? "" : sb.toString());
                }
                continue;
            }
            if (pendingLf) {
                pendingLf = false;
                if (buffer[pos] == '\n') {
                    pos++;
                    continue;
                }
            }
            int start = pos;
            while (pos < limit && buffer[pos] != '\n' && buffer[pos] != '\r') {
                pos++;
            }
            int chunk = pos - start;
            if (chunk > 0) {
                any = true;
                if (!tooLong) {
                    if (length + chunk > maxChars) {
                        tooLong = true;
                        sb = null;
                    } else {
                        if (sb == null) {
                            sb = new StringBuilder(Math.min(maxChars, Math.max(256, chunk)));
                        }
                        sb.append(buffer, start, chunk);
                        length += chunk;
                    }
                }
            }
            if (pos < limit) {
                char terminator = buffer[pos++];
                if (terminator == '\r') {
                    pendingLf = true;
                }
                lineNumber++;
                return new Line(lineNumber, tooLong ? null : sb == null ? "" : sb.toString());
            }
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
