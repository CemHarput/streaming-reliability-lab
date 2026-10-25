package io.github.cemharput.streamlab.loadgen.sse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class SseParserTest {

    private static final String GATEWAY_STREAM = "event:chunk\nid:0\ndata:{\"requestId\":\"r\",\"seq\":0,\"text\":\"h\u00e9llo \u20ac\",\"last\":false}\n\n"
            + "event:chunk\nid:1\ndata:{\"requestId\":\"r\",\"seq\":1,\"text\":\"x\",\"last\":true}\n\n"
            + "event:complete\ndata:{\"requestId\":\"r\",\"chunks\":2}\n\n";

    private static List<SseEvent> parse(byte[]... parts) {
        List<SseEvent> events = new ArrayList<>();
        SseParser parser = new SseParser((e, n) -> events.add(e));
        for (byte[] part : parts) {
            parser.feed(ByteBuffer.wrap(part), 0);
        }
        parser.end();
        return events;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static List<SseEvent> parse(String s) {
        return parse(bytes(s));
    }

    @Test
    void parsesGatewayFormatChunkAndCompleteEvents() {
        List<SseEvent> events = parse(GATEWAY_STREAM);

        assertThat(events).hasSize(3);
        assertThat(events.get(0).type()).isEqualTo("chunk");
        assertThat(events.get(0).id()).isEqualTo("0");
        assertThat(events.get(0).data()).contains("h\u00e9llo \u20ac");
        assertThat(events.get(1).id()).isEqualTo("1");
        assertThat(events.get(2)).isEqualTo(new SseEvent("complete", null, "{\"requestId\":\"r\",\"chunks\":2}"));
    }

    @Test
    void parsesErrorEvent() {
        List<SseEvent> events = parse("event:error\ndata:{\"requestId\":\"r\",\"code\":\"UNAVAILABLE\",\"message\":\"m\"}\n\n");

        assertThat(events).containsExactly(
                new SseEvent("error", null, "{\"requestId\":\"r\",\"code\":\"UNAVAILABLE\",\"message\":\"m\"}"));
    }

    @Test
    void everySplitPointGivesTheSameEventsForEveryLineEnding() {
        for (String eol : List.of("\n", "\r\n", "\r")) {
            byte[] input = bytes(GATEWAY_STREAM.replace("\n", eol));
            List<SseEvent> whole = parse(input);
            assertThat(whole).hasSize(3);
            for (int split = 0; split <= input.length; split++) {
                byte[] a = java.util.Arrays.copyOfRange(input, 0, split);
                byte[] b = java.util.Arrays.copyOfRange(input, split, input.length);
                assertThat(parse(a, b)).as("eol=%s split=%d", escape(eol), split).isEqualTo(whole);
            }
        }
    }

    @Test
    void everyTwoSplitPointsGiveTheSameEvents() {
        byte[] input = bytes("event: a\r\ndata: x\r\n\r\nid: 7\ndata: \u00fc\u20ac\rdata: y\r\r");
        List<SseEvent> whole = parse(input);
        assertThat(whole).hasSize(2);
        for (int i = 0; i <= input.length; i++) {
            for (int j = i; j <= input.length; j++) {
                byte[][] parts = {
                    java.util.Arrays.copyOfRange(input, 0, i),
                    java.util.Arrays.copyOfRange(input, i, j),
                    java.util.Arrays.copyOfRange(input, j, input.length)};
                assertThat(parse(parts)).as("split %d/%d", i, j).isEqualTo(whole);
            }
        }
    }

    @Test
    void oneByteAtATimeGivesTheSameEvents() {
        byte[] input = bytes(GATEWAY_STREAM);
        byte[][] parts = new byte[input.length][];
        for (int i = 0; i < input.length; i++) {
            parts[i] = new byte[] {input[i]};
        }
        assertThat(parse(parts)).isEqualTo(parse(input));
    }

    @Test
    void crlfSplitBetweenCrAndLfDoesNotProduceAnExtraBlankLine() {
        List<SseEvent> events = parse(bytes("data: a\r"), bytes("\ndata: b\r\n\r"), bytes("\n"));

        assertThat(events).containsExactly(new SseEvent("message", null, "a\nb"));
    }

    @Test
    void multiByteCharacterSplitAcrossReadsIsDecoded() {
        byte[] euro = bytes("data: \u20ac\n\n");
        // split inside the three byte euro sign
        List<SseEvent> events = parse(java.util.Arrays.copyOfRange(euro, 0, 7), java.util.Arrays.copyOfRange(euro, 7, euro.length));

        assertThat(events).containsExactly(new SseEvent("message", null, "\u20ac"));
    }

    @Test
    void leadingBomIsDropped() {
        assertThat(parse("\uFEFFdata: a\n\n")).containsExactly(new SseEvent("message", null, "a"));
    }

    @Test
    void bomInTheMiddleIsNotSpecial() {
        assertThat(parse("data: a\n\ndata: \uFEFFb\n\n")).extracting(SseEvent::data).containsExactly("a", "\uFEFFb");
    }

    @Test
    void commentsAreIgnored() {
        assertThat(parse(": keepalive\ndata: a\n: another\n\n")).containsExactly(new SseEvent("message", null, "a"));
    }

    @Test
    void multiLineDataIsJoinedWithNewline() {
        assertThat(parse("data: a\ndata: b\ndata:\ndata: c\n\n")).extracting(SseEvent::data).containsExactly("a\nb\n\nc");
    }

    @Test
    void lineWithoutColonIsAFieldNameWithEmptyValue() {
        // "data" alone appends an empty line to the data buffer, so the event is dispatched with empty data
        assertThat(parse("data\n\n")).containsExactly(new SseEvent("message", null, ""));
        // unknown field name without colon is ignored
        assertThat(parse("foo\ndata: x\n\n")).extracting(SseEvent::data).containsExactly("x");
    }

    @Test
    void onlyOneLeadingSpaceOfTheValueIsRemoved() {
        assertThat(parse("data:  two\n\n")).extracting(SseEvent::data).containsExactly(" two");
        assertThat(parse("data:none\n\n")).extracting(SseEvent::data).containsExactly("none");
    }

    @Test
    void unknownFieldsAndRetryAreIgnored() {
        assertThat(parse("retry: 1000\nfoo: bar\ndata: a\n\n")).containsExactly(new SseEvent("message", null, "a"));
    }

    @Test
    void blankLineWithoutDataDispatchesNothing() {
        assertThat(parse("event: x\nid: 5\n\n\n")).isEmpty();
    }

    @Test
    void eventTypeAndIdDoNotLeakIntoTheNextEvent() {
        assertThat(parse("event: a\nid: 1\ndata: x\n\ndata: y\n\n")).containsExactly(
                new SseEvent("a", "1", "x"), new SseEvent("message", null, "y"));
    }

    @Test
    void incompleteEventAtTheEndOfTheStreamIsDiscarded() {
        assertThat(parse("data: a\n\ndata: b\n")).extracting(SseEvent::data).containsExactly("a");
        assertThat(parse("data: a\n\ndata: b")).extracting(SseEvent::data).containsExactly("a");
    }

    @Test
    void timestampOfAnEventIsTheOneOfTheFeedThatCompletedIt() {
        List<Long> times = new ArrayList<>();
        SseParser parser = new SseParser((e, n) -> times.add(n));
        parser.feed(ByteBuffer.wrap(bytes("data: a\n")), 10);
        parser.feed(ByteBuffer.wrap(bytes("\ndata: b\n\ndata: c\n\n")), 20);

        assertThat(times).containsExactly(20L, 20L, 20L);
    }

    @Test
    void lineLongerThanTheLimitThrows() {
        SseParser parser = new SseParser((e, n) -> { }, 16);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(bytes("data: " + "x".repeat(40)));

        assertThatThrownBy(() -> parser.feed(ByteBuffer.wrap(out.toByteArray()), 0))
                .isInstanceOf(SseProtocolException.class);
    }

    @Test
    void eventDataLongerThanTheEventLimitThrowsEvenWhenEveryLineIsShort() {
        SseParser parser = new SseParser((e, n) -> { }, 16);
        StringBuilder in = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            in.append("data: ").append("x".repeat(8)).append('\n');
        }

        assertThatThrownBy(() -> parser.feed(ByteBuffer.wrap(bytes(in.toString())), 0))
                .isInstanceOf(SseProtocolException.class).hasMessageContaining("event data");
    }

    @Test
    void lineExactlyAtTheLimitIsAccepted() {
        List<SseEvent> events = new ArrayList<>();
        SseParser parser = new SseParser((e, n) -> events.add(e), 16);
        parser.feed(ByteBuffer.wrap(bytes("data: " + "x".repeat(10) + "\n\n")), 0);

        assertThat(events).hasSize(1);
    }

    private static String escape(String s) {
        return s.replace("\r", "\\r").replace("\n", "\\n");
    }
}
