package software.spool.dsl.variables;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DescriptorVariablesTest {

    private final EnvironmentValues seen = new EnvironmentValues();

    @Test
    void resolve_twoReferencesInOneText_replacesBoth() {
        Map<String, Object> tree = resolve(Map.of("host", "spool.local", "port", 8080), Map.of("url", "http://${host}:${port}/api"));

        assertThat(tree).containsEntry("url", "http://spool.local:8080/api");
    }

    @Test
    void resolve_escapedReferenceNextToARealOne_keepsTheEscapedOneAsText() {
        Map<String, Object> tree = resolve(Map.of("root", "/data"), Map.of("path", "$${root}/${root}"));

        assertThat(tree).containsEntry("path", "${root}//data");
    }

    @Test
    void resolve_valuesInsideListsAndNestedObjects_areReplacedButKeysAreNot() {
        Map<String, Object> modules = Map.of("crawler", Map.of("id", "${name}", "${name}", "kept"));

        Map<String, Object> tree = resolve(Map.of("name", "boe"), Map.of("modules", List.of(modules)));

        assertThat(tree.get("modules")).isEqualTo(List.of(Map.of("crawler", Map.of("id", "boe", "${name}", "kept"))));
    }

    @Test
    void resolve_valuesThatAreNotText_areLeftAsTheyAre() {
        Map<String, Object> tree = resolve(Map.of(), Map.of("retries", 3, "enabled", true));

        assertThat(tree).containsEntry("retries", 3).containsEntry("enabled", true);
    }

    @Test
    void resolve_variablesBlock_isNotPartOfTheResult() {
        Map<String, Object> tree = resolve(Map.of("root", "/data"), Map.of("path", "${root}"));

        assertThat(tree).doesNotContainKey("variables");
    }

    @Test
    void resolve_unclosedReference_failsNamingThePath() {
        assertThatThrownBy(() -> resolve(Map.of(), Map.of("path", "/data/${root")))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("path")
                .hasMessageContaining("${root");
    }

    @Test
    void resolve_variableThatIsAList_failsNamingTheVariable() {
        assertThatThrownBy(() -> resolve(Map.of("hosts", List.of("a", "b")), Map.of()))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("variables.hosts")
                .hasMessageContaining("single value");
    }

    @Test
    void resolve_unusedVariableWithAnUnknownReference_stillFails() {
        assertThatThrownBy(() -> resolve(Map.of("unused", "${missing}"), Map.of()))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("variables.unused")
                .hasMessageContaining("${missing}");
    }

    @Test
    void mask_environmentValueInAMessage_isReplacedByItsReference() {
        DescriptorVariables variables = new DescriptorVariables(name -> name.equals("TOKEN") ? "abc123" : null, seen);
        variables.resolve(tree(Map.of(), Map.of("token", "${env:TOKEN}")));

        assertThat(seen.mask("token must be short, got 'abc123'")).isEqualTo("token must be short, got '${env:TOKEN}'");
    }

    private Map<String, Object> resolve(Map<String, Object> variables, Map<String, Object> values) {
        return new DescriptorVariables(name -> null, seen).resolve(tree(variables, values));
    }

    private static Map<String, Object> tree(Map<String, Object> variables, Map<String, Object> values) {
        Map<String, Object> tree = new LinkedHashMap<>(values);
        if (!variables.isEmpty()) tree.put("variables", new LinkedHashMap<>(variables));
        return tree;
    }
}
