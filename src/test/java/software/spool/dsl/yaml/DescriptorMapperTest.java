package software.spool.dsl.yaml;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.ErrorRouterDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.yaml.raw.RawPieceDescriptor;
import software.spool.dsl.yaml.raw.RawInfrastructureDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DescriptorMapperTest {

    @Test
    void map_ingesterModule_returnsIngesterDescriptor() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(
            anyInfra(),
            List.of(Map.of("ingester", Map.of("type", "REACTIVE", "id", "test-ingester")))
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
            Map.of("ingester", Map.of("type", "REACTIVE", "id", "i")),
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
    void map_janitorWithAnIntervalThatIsNotADuration_namesTheKeyAndTheValue() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("janitor",
            Map.of("id", "j", "schedule", Map.of("every", "abc")))));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].janitor.schedule.every must be a duration such as 30d or P30D, got 'abc'");
    }

    @Test
    void map_crawlerWithAScheduleThatIsNotADuration_namesTheKeyAndTheValue() {
        Map<String, Object> crawler = crawler(Map.of("namingConvention", "SNAKE_CASE"));
        crawler.put("schedule", Map.of("every", "soon"));
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("crawler", crawler)));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].crawler.schedule.every must be a duration such as 30d or P30D, got 'soon'");
    }

    @Test
    void map_crawlerWithAnErrorRouter_keepsItsTypeAndConfiguration() {
        Map<String, Object> crawler = crawler(Map.of("namingConvention", "SNAKE_CASE"));
        crawler.put("errorRouter", Map.of("type", "ALERTS", "configuration", Map.of("webhook", "https://alerts")));
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("crawler", crawler)));

        CrawlerDescriptor result = (CrawlerDescriptor) DescriptorMapper.map(raw).modules().get(0);

        assertThat(result.errorRouter()).isEqualTo(new ErrorRouterDescriptor("ALERTS", Map.of("webhook", "https://alerts")));
    }

    @Test
    void map_crawlerWithoutAnErrorRouter_hasNone() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(),
            List.of(Map.of("crawler", crawler(Map.of("namingConvention", "SNAKE_CASE")))));

        CrawlerDescriptor result = (CrawlerDescriptor) DescriptorMapper.map(raw).modules().get(0);

        assertThat(result.errorRouter()).isNull();
    }

    @Test
    void map_errorRouterWithoutConfiguration_hasAnEmptyOne() {
        Map<String, Object> crawler = crawler(Map.of("namingConvention", "SNAKE_CASE"));
        crawler.put("errorRouter", Map.of("type", "ALERTS"));
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("crawler", crawler)));

        CrawlerDescriptor result = (CrawlerDescriptor) DescriptorMapper.map(raw).modules().get(0);

        assertThat(result.errorRouter().configuration()).isEmpty();
    }

    @Test
    void map_errorRouterWithoutAType_namesTheMissingKey() {
        Map<String, Object> crawler = crawler(Map.of("namingConvention", "SNAKE_CASE"));
        crawler.put("errorRouter", Map.of("configuration", Map.of("webhook", "https://alerts")));
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(anyInfra(), List.of(Map.of("crawler", crawler)));

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .hasMessage("modules[0].crawler.errorRouter.type is required");
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
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(new RawInfrastructureDescriptor(Map.of(
            "bus", piece(InfrastructureKind.EVENT_BUS, "IN_MEMORY"),
            "inbox", piece(InfrastructureKind.INBOX, "IN_MEMORY"))), List.of());

        SpoolNodeDescriptor result = DescriptorMapper.map(raw);

        assertThat(result.infrastructure().ofKind(InfrastructureKind.DATA_LAKE)).isEmpty();
        assertThat(result.infrastructure().piece("inbox").orElseThrow().type()).isEqualTo("IN_MEMORY");
    }

    @Test
    void map_withoutInfrastructure_saysSo() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(null, List.of());

        assertThatThrownBy(() -> DescriptorMapper.map(raw)).hasMessage("infrastructure is required");
    }

    @Test
    void map_infrastructureComponentWithoutType_namesTheKey() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(new RawInfrastructureDescriptor(Map.of(
            "inbox", piece(InfrastructureKind.INBOX, null))), List.of());

        assertThatThrownBy(() -> DescriptorMapper.map(raw)).hasMessage("infrastructure.inbox.type is required");
    }

    /** A crawler with everything it needs except the given event mapping. */
    private static Map<String, Object> crawler(Map<String, Object> eventMapping) {
        Map<String, Object> source = new HashMap<>(Map.of("type", "HTTP", "normalizer", "JSON_ARRAY", "mediaType", "JSON"));
        return new HashMap<>(Map.of("type", "POLL", "id", "c", "source", source, "eventMapping", eventMapping));
    }

    private static RawInfrastructureDescriptor anyInfra() {
        return new RawInfrastructureDescriptor(Map.of(
            "bus", piece(InfrastructureKind.EVENT_BUS, "in-memory"),
            "inbox", piece(InfrastructureKind.INBOX, "in-memory"),
            "lake", piece(InfrastructureKind.DATA_LAKE, "in-memory")));
    }

    private static RawPieceDescriptor piece(InfrastructureKind kind, String type) {
        return new RawPieceDescriptor(Optional.of(kind), type, null);
    }
}
