package io.github.cemharput.streamlab.loadgen.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class FlatJsonTest {

    @Test
    void parsesGatewayPayloads() {
        assertThat(FlatJson.parse("{\"requestId\":\"abc\",\"seq\":3,\"text\":\"t3\",\"last\":false}"))
                .containsEntry("requestId", "abc").containsEntry("seq", 3L).containsEntry("text", "t3")
                .containsEntry("last", false);
        assertThat(FlatJson.parse("{\"requestId\":\"abc\",\"chunks\":10}")).containsEntry("chunks", 10L);
        assertThat(FlatJson.parse("{\"requestId\":\"a\",\"code\":\"UNAVAILABLE\",\"message\":\"gone\"}"))
                .containsEntry("code", "UNAVAILABLE").containsEntry("message", "gone");
    }

    @Test
    void parsesNumbersNullsWhitespaceAndEmptyObject() {
        Map<String, Object> m = FlatJson.parse(" { \"a\" : -12 , \"b\":1.5e2 , \"c\": null,\n\"d\":true } ");

        assertThat(m).containsEntry("a", -12L).containsEntry("b", 150.0).containsEntry("c", null)
                .containsEntry("d", true);
        assertThat(FlatJson.parse("{}")).isEmpty();
    }

    @Test
    void parsesEscapesIncludingUnicode() {
        Map<String, Object> m = FlatJson.parse("{\"s\":\"a\\\"b\\\\c\\/\\b\\f\\n\\r\\t\\u00fc\\u20AC\\ud83d\\ude00\"}");

        assertThat(m.get("s")).isEqualTo("a\"b\\c/\b\f\n\r\t\u00fc\u20ac\ud83d\ude00");
    }

    @Test
    void keepsKeyOrder() {
        assertThat(List.copyOf(FlatJson.parse("{\"z\":1,\"a\":2,\"m\":3}").keySet())).containsExactly("z", "a", "m");
    }

    @Test
    void rejectsMalformedInput() {
        for (String bad : List.of("", "[]", "{", "{\"a\"}", "{\"a\":}", "{\"a\":1,}", "{\"a\":1 \"b\":2}",
                "{\"a\":\"x}", "{\"a\":\"\\q\"}", "{\"a\":\"\\u12\"}", "{\"a\":\"\\uZZZZ\"}", "{\"a\":1}x",
                "{\"a\":{}}", "{\"a\":[1]}", "{\"a\":1,\"a\":2}", "{\"a\":tru}", "{\"a\":--1}", "{\"a\":\"\n\"}",
                "{a:1}", "nope")) {
            assertThatThrownBy(() -> FlatJson.parse(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> FlatJson.parse(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
