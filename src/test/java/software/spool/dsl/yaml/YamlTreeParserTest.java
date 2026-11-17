package software.spool.dsl.yaml;

import org.junit.jupiter.api.Test;
import software.spool.core.exception.DeserializationException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class YamlTreeParserTest {

    private final YamlTreeParser parser = new YamlTreeParser();

    @Test
    void deserialize_aliasOfAnAnchor_repeatsTheAnchoredBlock() {
        Map<String, Object> tree = parser.deserialize("""
                inbox: &local {type: FILE_SYSTEM, path: /data}
                dataLake: *local
                """);

        assertThat(tree.get("dataLake")).isEqualTo(Map.of("type", "FILE_SYSTEM", "path", "/data"));
    }

    @Test
    void deserialize_scalars_keepTheTypesTheDescriptorHasAlwaysHad() {
        Map<String, Object> tree = parser.deserialize("""
                retries: 3
                enabled: true
                ratio: 0.5
                since: 2026-01-01
                """);

        assertThat(tree).containsEntry("retries", 3)
                .containsEntry("enabled", true)
                .containsEntry("ratio", 0.5)
                .containsEntry("since", "2026-01-01");
    }

    @Test
    void deserialize_emptyDocument_isAnEmptyTree() {
        assertThat(parser.deserialize("")).isEmpty();
    }

    @Test
    void deserialize_documentThatIsNotAnObject_failsSayingSo() {
        assertThatThrownBy(() -> parser.deserialize("- a\n- b\n"))
                .isInstanceOf(DeserializationException.class)
                .hasMessageContaining("object");
    }

    @Test
    void deserialize_brokenYaml_failsWithTheLineOfTheProblem() {
        assertThatThrownBy(() -> parser.deserialize("a: [1, 2\nb: 3\n"))
                .isInstanceOf(DeserializationException.class)
                .hasMessageContaining("line");
    }
}
