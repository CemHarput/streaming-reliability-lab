package io.github.cemharput.streamlab.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cemharput.streamlab.bench.json.Json;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class JsonTest {

    @Test
    void parsesNestedStructuresAndKeepsKeyOrder() {
        Map<String, Object> m = Json.parseObject(
                "{\"b\":1,\"a\":[true,false,null,\"x\",{\"n\":2.5}],\"c\":{\"d\":-7}}");
        assertThat(m.keySet()).containsExactly("b", "a", "c");
        assertThat(m.get("b")).isEqualTo(1L);
        List<?> list = (List<?>) m.get("a");
        assertThat(list).hasSize(5);
        assertThat(list.get(0)).isEqualTo(true);
        assertThat(list.get(2)).isNull();
        assertThat(((Map<?, ?>) list.get(4)).get("n")).isEqualTo(2.5);
        assertThat(((Map<?, ?>) m.get("c")).get("d")).isEqualTo(-7L);
    }

    @Test
    void roundTripsEscapesAndControlCharacters() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("text", "quote \" backslash \\ newline \n tab \t bell \u0007 unicode \u00e9");
        m.put("empty", new ArrayList<>());
        m.put("emptyMap", new LinkedHashMap<>());
        m.put("nothing", null);
        m.put("big", 9_000_000_000L);
        m.put("ratio", 0.25);
        Map<String, Object> back = Json.parseObject(Json.write(m));
        assertThat(back).isEqualTo(m);
        assertThat(Json.writeCompact(m)).doesNotContain("\n").contains("\\u0007");
    }

    @Test
    void parsesUnicodeEscapes() {
        assertThat(Json.parse("\"\\u0041\\n\"")).isEqualTo("A\n");
    }

    @Test
    void prettyOutputIsIndentedAndEndsWithNewline() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", List.of(1, 2));
        assertThat(Json.write(m)).isEqualTo("{\n  \"a\": [\n    1,\n    2\n  ]\n}\n");
    }

    @Test
    void nonFiniteNumbersBecomeNull() {
        assertThat(Json.writeCompact(Map.of("v", Double.NaN))).isEqualTo("{\"v\":null}");
    }

    @Test
    void rejectsMalformedInput() {
        for (String bad : List.of("", "{", "{\"a\":}", "[1,]", "{\"a\" 1}", "\"abc", "tru", "{} x", "[1 2]",
                "\"\\q\"", "\"a\nb\"", "-", "{1:2}")) {
            assertThatThrownBy(() -> Json.parse(bad)).as(bad).isInstanceOf(Json.JsonException.class);
        }
    }

    @Test
    void rejectsExcessiveNesting() {
        String deep = "[".repeat(200) + "]".repeat(200);
        assertThatThrownBy(() -> Json.parse(deep)).isInstanceOf(Json.JsonException.class)
                .hasMessageContaining("nesting");
    }

    @Test
    void parseObjectRejectsNonObjects() {
        assertThatThrownBy(() -> Json.parseObject("[1]")).isInstanceOf(Json.JsonException.class);
    }
}
