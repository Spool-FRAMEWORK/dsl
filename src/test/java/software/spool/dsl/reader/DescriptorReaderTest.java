package software.spool.dsl.reader;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DescriptorReaderTest {

    @Test
    void string_missingKey_namesTheKeyAndItsPath() {
        DescriptorReader reader = DescriptorReader.of("modules[0].crawler", Map.of());

        assertThatThrownBy(() -> reader.string("id"))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessage("modules[0].crawler.id is required");
    }

    @Test
    void string_nullValue_countsAsMissing() {
        Map<String, Object> values = new HashMap<>();
        values.put("id", null);

        assertThatThrownBy(() -> DescriptorReader.of("janitor", values).string("id"))
                .hasMessage("janitor.id is required");
    }

    @Test
    void string_aNumber_isReadAsText() {
        assertThat(DescriptorReader.of("x", Map.of("id", 5)).string("id")).isEqualTo("5");
    }

    @Test
    void required_wrongShape_saysWhatWasExpectedAndWhatWasFound() {
        DescriptorReader reader = DescriptorReader.of("janitor.configuration", Map.of("milliseconds", "abc"));

        assertThatThrownBy(() -> reader.required("milliseconds", ValueParsers.INTEGER))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessage("janitor.configuration.milliseconds must be an integer, got 'abc'");
    }

    @Test
    void optional_missingKey_isEmpty() {
        assertThat(DescriptorReader.of("x", Map.of()).optional("size", ValueParsers.INTEGER)).isEmpty();
    }

    @Test
    void optional_presentButWrongShape_stillFails() {
        DescriptorReader reader = DescriptorReader.of("x", Map.of("size", "many"));

        assertThatThrownBy(() -> reader.optional("size", ValueParsers.INTEGER))
                .hasMessage("x.size must be an integer, got 'many'");
    }

    @Test
    void object_missingKey_isRequired() {
        assertThatThrownBy(() -> DescriptorReader.of("modules[0].crawler", Map.of()).object("source"))
                .hasMessage("modules[0].crawler.source is required");
    }

    @Test
    void object_notAnObject_fails() {
        DescriptorReader reader = DescriptorReader.of("crawler", Map.of("source", "http"));

        assertThatThrownBy(() -> reader.object("source"))
                .hasMessage("crawler.source must be an object, got 'http'");
    }

    @Test
    void objectOrEmpty_missingKey_readsAsAnEmptyObject() {
        DescriptorReader janitor = DescriptorReader.of("modules[0]", Map.of()).objectOrEmpty("janitor");

        assertThatThrownBy(() -> janitor.string("id")).hasMessage("modules[0].janitor.id is required");
    }

    @Test
    void nestedObjects_accumulateTheirPath() {
        DescriptorReader root = DescriptorReader.of("", Map.of("crawler", Map.of("source", Map.of())));

        assertThatThrownBy(() -> root.object("crawler").object("source").string("type"))
                .hasMessage("crawler.source.type is required");
    }

    @Test
    void list_elementsKeepTheirIndexInThePath() {
        DescriptorReader source = DescriptorReader.of("crawler.source", Map.of("enrichment", List.of(
                Map.of("source", "a", "target", "b"),
                Map.of("source", "c"))));

        List<DescriptorReader> rules = source.list("enrichment");

        assertThat(rules).hasSize(2);
        assertThat(rules.get(0).string("target")).isEqualTo("b");
        assertThatThrownBy(() -> rules.get(1).string("target"))
                .hasMessage("crawler.source.enrichment[1].target is required");
    }

    @Test
    void list_missingKey_isEmpty() {
        assertThat(DescriptorReader.of("x", Map.of()).list("enrichment")).isEmpty();
    }

    @Test
    void list_notAList_fails() {
        assertThatThrownBy(() -> DescriptorReader.of("x", Map.of("enrichment", "none")).list("enrichment"))
                .hasMessage("x.enrichment must be a list, got 'none'");
    }

    @Test
    void stringMap_keepsPlainValuesAsText() {
        Map<String, Object> configuration = new LinkedHashMap<>();
        configuration.put("size", 5);
        configuration.put("async", true);

        Map<String, String> result = DescriptorReader.of("x", Map.of("configuration", configuration))
                .stringMap("configuration");

        assertThat(result).containsEntry("size", "5").containsEntry("async", "true");
    }

    @Test
    void stringMap_missingKey_isEmpty() {
        assertThat(DescriptorReader.of("x", Map.of()).stringMap("configuration")).isEmpty();
    }

    @Test
    void keys_keepTheOrderOfTheDescriptor() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("b", 1);
        values.put("a", 2);

        assertThat(DescriptorReader.of("x", values).keys()).containsExactly("b", "a");
    }

    @Test
    void ofValue_somethingThatIsNotAnObject_fails() {
        assertThatThrownBy(() -> DescriptorReader.ofValue("modules[0]", "crawler"))
                .hasMessage("modules[0] must be an object, got 'crawler'");
    }

    @Test
    void require_nullValue_failsWithThePath() {
        assertThatThrownBy(() -> DescriptorReader.require("infrastructure", null))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessage("infrastructure is required");
    }

    @Test
    void anotherKindOfValue_needsOnlyANewParser_notAChangeInTheReader() {
        ValueParser<Duration> seconds = ValueParsers.of("a number of seconds", raw ->
                ValueParsers.INTEGER.parse(raw).map(Duration::ofSeconds));
        DescriptorReader reader = DescriptorReader.of("janitor", Map.of("timeout", 30, "ttl", "soon"));

        assertThat(reader.required("timeout", seconds)).isEqualTo(Duration.ofSeconds(30));
        assertThatThrownBy(() -> reader.required("ttl", seconds))
                .hasMessage("janitor.ttl must be a number of seconds, got 'soon'");
        assertThat(reader.optional("missing", seconds)).isEqualTo(Optional.empty());
    }
}
