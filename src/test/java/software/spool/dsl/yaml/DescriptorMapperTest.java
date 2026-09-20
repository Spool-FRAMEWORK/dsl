package software.spool.dsl.yaml;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.yaml.raw.RawComponentDescriptor;
import software.spool.dsl.yaml.raw.RawInfrastructureDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DescriptorMapperTest {

    @Test
    void map_ingesterModule_returnsIngesterDescriptor() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(
            anyInfra(),
            List.of(Map.of("ingester", Map.of("type", "polling", "id", "test-ingester")))
        );

        SpoolNodeDescriptor result = DescriptorMapper.map(raw);

        assertThat(result.modules()).hasSize(1);
        assertThat(result.modules().get(0)).isInstanceOf(IngesterDescriptor.class);
        assertThat(((IngesterDescriptor) result.modules().get(0)).id()).isEqualTo("test-ingester");
    }

    @Test
    void map_unknownModuleType_throwsIllegalArgumentException() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(
            anyInfra(),
            List.of(Map.of("unknown", Map.of("id", "x")))
        );

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void map_moduleWithoutId_namesTheMissingKey() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("janitor", Map.of())));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessage("modules[0].janitor.id is required");
    }

    @Test
    void map_moduleWithAnEmptyBody_namesTheFirstMissingKey() {
        Map<String, Object> module = new HashMap<>();
        module.put("janitor", null);
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(module));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].janitor.id is required");
    }

    @Test
    void map_secondModuleWithAProblem_usesItsIndexInThePath() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(
            Map.of("ingester", Map.of("id", "i")),
            Map.of("janitor", Map.of())));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[1].janitor.id is required");
    }

    @Test
    void map_crawlerWithoutSource_namesTheMissingKey() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(),
            List.of(Map.of("crawler", Map.of("type", "POLL", "id", "c"))));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].crawler.source is required");
    }

    @Test
    void map_crawlerWithoutNamingConvention_namesTheMissingKey() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("crawler", crawler(Map.of()))));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].crawler.eventMapping.namingConvention is required");
    }

    @Test
    void map_crawlerWithAnUnknownNamingConvention_listsTheValidOnes() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(),
            List.of(Map.of("crawler", crawler(Map.of("namingConvention", "kebab")))));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessageStartingWith("modules[0].crawler.eventMapping.namingConvention must be one of ")
            .hasMessageContaining("SNAKE_CASE")
            .hasMessageEndingWith("got 'kebab'");
    }

    @Test
    void map_janitorWithANonNumericInterval_namesTheKeyAndTheValue() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("janitor",
            Map.of("id", "j", "configuration", Map.of("milliseconds", "abc")))));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].janitor.configuration.milliseconds must be an integer, got 'abc'");
    }

    @Test
    void map_crawlerWithANonNumericSchedule_namesTheKeyAndTheValue() {
        Map<String, Object> crawler = crawler(Map.of("namingConvention", "SNAKE_CASE"));
        ((Map<String, Object>) crawler.get("source")).put("configuration", Map.of("scheduleMilliseconds", "soon"));
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("crawler", crawler)));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].crawler.source.configuration.scheduleMilliseconds must be an integer, got 'soon'");
    }

    @Test
    void map_unknownModuleType_listsTheKnownOnes() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("unknown", Map.of("id", "x"))));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageContaining("modules[0] has an unknown module type 'unknown'")
            .hasMessageContaining("Known types: crawler, ingester, janitor");
    }

    @Test
    void map_entryWithTwoModuleKeys_asksForExactlyOne() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(),
            List.of(Map.of("janitor", Map.of("id", "j"), "ingester", Map.of("id", "i"))));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessageContaining("modules[0] must have exactly one key naming the module type");
    }

    @Test
    void map_withoutModules_saysSo() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), null);

        assertThatThrownBy(() -> DescriptorMapper.map(raw)).hasMessage("modules is required");
    }

    @Test
    void map_infrastructureWithoutDataLake_isAcceptedBecauseOnlyTheIngesterNeedsIt() {
        RawComponentDescriptor component = new RawComponentDescriptor("IN_MEMORY", null);
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(
            new RawInfrastructureDescriptor(null, component, component, null), List.of());

        SpoolNodeDescriptor result = DescriptorMapper.map(raw);

        assertThat(result.infrastructure().dataLake()).isNull();
        assertThat(result.infrastructure().inbox().type()).isEqualTo("IN_MEMORY");
    }

    @Test
    void map_withoutInfrastructure_saysSo() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(null, List.of());

        assertThatThrownBy(() -> DescriptorMapper.map(raw)).hasMessage("infrastructure is required");
    }

    @Test
    void map_infrastructureComponentWithoutType_namesTheKey() {
        RawComponentDescriptor component = new RawComponentDescriptor("IN_MEMORY", null);
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(
            new RawInfrastructureDescriptor(null, component, new RawComponentDescriptor(null, null), component), List.of());

        assertThatThrownBy(() -> DescriptorMapper.map(raw)).hasMessage("infrastructure.inbox.type is required");
    }

    /** A crawler with everything it needs except the given event mapping. */
    private static Map<String, Object> crawler(Map<String, Object> eventMapping) {
        Map<String, Object> source = new HashMap<>(Map.of("type", "HTTP", "mediaType", "JSON_ARRAY"));
        return new HashMap<>(Map.of("type", "POLL", "id", "c", "source", source, "eventMapping", eventMapping));
    }

    private static RawInfrastructureDescriptor anyInfra() {
        RawComponentDescriptor component = new RawComponentDescriptor("in-memory", null);
        return new RawInfrastructureDescriptor(null, component, component, component);
    }
}
