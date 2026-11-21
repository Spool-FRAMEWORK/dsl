package software.spool.dsl.providers.crawler;

import software.spool.dsl.Pieces;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.port.serde.NamingConvention;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.SpoolNodeDSL;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.ErrorRouterDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.core.utils.media.MediaTypes;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Extended;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CrawlerErrorRouterTest {

    @BeforeEach
    void forgetEarlierRouters() {
        RecordingErrorRouterProvider.CREATED.clear();
    }

    @Test
    void pollCrawler_withAnErrorRouter_asksThePluginForItWithTheConfigurationAndTheModuleId(@TempDir Path tmp) {
        SpoolNodeDescriptor node = node("POLL", new ErrorRouterDescriptor("test_alerts", Map.of("webhook", "https://alerts")), tmp);

        SpoolNodeDSL.fromDescriptor(node);

        assertThat(RecordingErrorRouterProvider.CREATED).hasSize(1);
        PluginConfiguration asked = RecordingErrorRouterProvider.CREATED.get(0);
        assertThat(asked.require("webhook")).isEqualTo("https://alerts");
        assertThat(asked.require("moduleId", String.class)).isEqualTo("c");
    }

    @Test
    void streamCrawler_withAnErrorRouter_asksThePluginForIt(@TempDir Path tmp) {
        SpoolNodeDescriptor node = node("STREAM", new ErrorRouterDescriptor("TEST_ALERTS", Map.of()), tmp);

        SpoolNodeDSL.fromDescriptor(node);

        assertThat(RecordingErrorRouterProvider.CREATED).hasSize(1);
    }

    @Test
    void pollCrawler_withoutAnErrorRouter_asksNoPlugin(@TempDir Path tmp) {
        SpoolNodeDSL.fromDescriptor(node("POLL", null, tmp));

        assertThat(RecordingErrorRouterProvider.CREATED).isEmpty();
    }

    @Test
    void pollCrawler_withTheDefaultErrorRouter_isBuilt(@TempDir Path tmp) {
        SpoolNodeDescriptor node = node("POLL", new ErrorRouterDescriptor("DEFAULT", Map.of()), tmp);

        assertThat(SpoolNodeDSL.fromDescriptor(node)).isNotNull();
    }

    @Test
    void pollCrawler_withAnUnknownErrorRouter_namesTheKeyTheModuleAndTheKnownOnes(@TempDir Path tmp) {
        SpoolNodeDescriptor node = node("POLL", new ErrorRouterDescriptor("NOPE", Map.of()), tmp);

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(node))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessage("module 'c': errorRouter.type 'NOPE' is not a known error router. Known: DEFAULT, TEST_ALERTS");
    }

    private static SpoolNodeDescriptor node(String crawlerType, ErrorRouterDescriptor errorRouter, Path tmp) {
        String sourceType = crawlerType.equals("POLL") ? "HTTP" : "IN_MEMORY";
        Map<String, String> sourceConfiguration = crawlerType.equals("POLL")
            ? Map.of("sourceId", "s", "url", "http://localhost:1/events")
            : Map.of("sourceId", "s", "eventClassName", "StreamedOrder");
        SourceDescriptor source = new SourceDescriptor(sourceType, "s", sourceConfiguration, Extended.named("JSON_ARRAY", "source.normalizer"), MediaTypes.JSON, "", List.of());
        EventMappingDescriptor mapping = new EventMappingDescriptor(NamingConvention.SNAKE_CASE, List.of(), List.of());
        return Pieces.node(List.of(new CrawlerDescriptor(crawlerType, "c", source, mapping, errorRouter, Optional.empty())),
            Pieces.eventBus("IN_MEMORY"), Pieces.inbox("FILE_SYSTEM", Map.of("path", tmp.toString())));
    }
}
