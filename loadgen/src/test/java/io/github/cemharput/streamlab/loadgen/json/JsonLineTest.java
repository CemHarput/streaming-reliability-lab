package io.github.cemharput.streamlab.loadgen.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
class JsonLineTest {

    @Test
    void fieldsKeepInsertionOrderAndTypes() {
        String json = new JsonLine().field("a", "x").field("b", 5L).field("c", true).field("d", 1.5)
                .field("e", (String) null).field("f", (Long) null).field("g", Long.valueOf(7)).nullField("h").toString();

        assertThat(json).isEqualTo("{\"a\":\"x\",\"b\":5,\"c\":true,\"d\":1.5,\"e\":null,\"f\":null,\"g\":7,\"h\":null}");
    }

    @Test
    void emptyObject() {
        assertThat(new JsonLine().toString()).isEqualTo("{}");
    }

    @Test
    void escapesQuoteBackslashAndControlCharacters() {
        String json = new JsonLine().field("s", "a\"b\\c\b\f\n\r\t\u0001\u001f").toString();

        assertThat(json).isEqualTo("{\"s\":\"a\\\"b\\\\c\\b\\f\\n\\r\\t\\u0001\\u001f\"}");
    }

    @Test
    void nonAsciiAndSlashAreWrittenAsIs() {
        assertThat(new JsonLine().field("s", "ü€/😀").toString())
                .isEqualTo("{\"s\":\"ü€/😀\"}");
    }

    @Test
    void keysAreEscapedToo() {
        assertThat(new JsonLine().field("a\"b", 1L).toString()).isEqualTo("{\"a\\\"b\":1}");
    }

    @Test
    void nanAndInfinityAreRejected() {
        assertThatThrownBy(() -> new JsonLine().field("x", Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JsonLine().field("x", Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void outputIsASingleLine() {
        assertThat(new JsonLine().field("s", "a\nb\r\nc").toString()).doesNotContain("\n").doesNotContain("\r");
    }

    @Test
    void roundTripsThroughFlatJson() {
        String tricky = "q\"\\ \n\t\u0000\u001f ü€😀 </script>";
        Map<String, Object> parsed = FlatJson.parse(new JsonLine().field("s", tricky).field("n", Long.MIN_VALUE)
                .field("d", -0.25).field("b", false).nullField("z").toString());

        assertThat(parsed).containsEntry("s", tricky).containsEntry("n", Long.MIN_VALUE).containsEntry("d", -0.25)
                .containsEntry("b", false).containsEntry("z", null);
    }
}
