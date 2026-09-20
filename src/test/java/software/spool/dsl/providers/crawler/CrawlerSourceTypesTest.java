package software.spool.dsl.providers.crawler;

import org.junit.jupiter.api.Test;
import software.spool.core.port.serde.NamingConvention;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.SpoolNodeDSL;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CrawlerSourceTypesTest {

    private static final InfrastructureComponentDescriptor BUS = new InfrastructureComponentDescriptor("IN_MEMORY", Map.of());

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
        SourceDescriptor source = new SourceDescriptor(sourceType, "s", Map.of(), "JSON_ARRAY", "", List.of());
        EventMappingDescriptor mapping = new EventMappingDescriptor(NamingConvention.SNAKE_CASE, List.of(), List.of());
        InfrastructureDescriptor infra = new InfrastructureDescriptor(null, BUS, BUS, BUS);
        return new SpoolNodeDescriptor(infra, List.of(new CrawlerDescriptor(crawlerType, "c", source, mapping)));
    }
}
