package software.spool.dsl.providers.crawler;

import software.spool.dsl.Pieces;
import org.junit.jupiter.api.Test;
import software.spool.core.port.serde.NamingConvention;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.SpoolNodeDSL;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.core.utils.media.MediaTypes;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Extended;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CrawlerSourceTypesTest {


    @Test
    void pollCrawler_withAnUnknownSourceType_namesTheKeyTheModuleAndTheKnownSources() {
        SpoolNodeDescriptor node = node("POLL", "NOPE");

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(node))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("module 'c': source.type 'NOPE' is not a known poll source. Known: ")
            .hasMessageContaining("HTTP");
    }

    @Test
    void streamCrawler_withAnUnknownSourceType_namesTheKeyTheModuleAndTheKnownSources() {
        SpoolNodeDescriptor node = node("STREAM", "NOPE");

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(node))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("module 'c': source.type 'NOPE' is not a known stream source. Known: ")
            .hasMessageContaining("IN_MEMORY");
    }

    private static SpoolNodeDescriptor node(String crawlerType, String sourceType) {
        SourceDescriptor source = new SourceDescriptor(sourceType, "s", Map.of(), Extended.named("JSON_ARRAY", "source.normalizer"), MediaTypes.JSON, "", List.of());
        EventMappingDescriptor mapping = new EventMappingDescriptor(NamingConvention.SNAKE_CASE, List.of(), List.of());
        return Pieces.node(List.of(new CrawlerDescriptor(crawlerType, "c", source, mapping)),
            Pieces.eventBus("IN_MEMORY"), Pieces.inbox("IN_MEMORY", Map.of()));
    }
}
