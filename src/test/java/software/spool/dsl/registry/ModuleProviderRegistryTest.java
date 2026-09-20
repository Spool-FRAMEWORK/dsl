package software.spool.dsl.registry;

import org.junit.jupiter.api.Test;
import software.spool.core.port.serde.NamingConvention;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ModuleProviderRegistryTest {

    private static final InfrastructureComponentDescriptor BUS = new InfrastructureComponentDescriptor("IN_MEMORY", Map.of());
    private static final InfrastructureDescriptor INFRA = new InfrastructureDescriptor(null, BUS, BUS, BUS);

    @Test
    void build_aCrawlerOfATypeNobodySupports_namesTheModuleAndTheSupportedTypes() {
        CrawlerDescriptor crawler = crawler("POLLING");

        assertThatThrownBy(() -> ModuleProviderRegistry.build(crawler, INFRA))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("module 'c' of type CRAWLER_POLLING is not supported. Supported types: ")
            .hasMessageContaining("CRAWLER_POLL")
            .hasMessageContaining("CRAWLER_STREAM");
    }

    @Test
    void build_aCrawlerOfATypeNobodySupports_listsTheSupportedTypesSorted() {
        String message = catchMessage(crawler("POLLING"));

        List<String> supported = Arrays.asList(message.substring(message.indexOf("Supported types: ") + 17).split(", "));

        assertThat(supported).isSorted().contains("CRAWLER_POLL", "CRAWLER_STREAM", "INGESTER", "JANITOR");
    }

    private static String catchMessage(CrawlerDescriptor crawler) {
        try {
            ModuleProviderRegistry.build(crawler, INFRA);
        } catch (InvalidDescriptorException e) {
            return e.getMessage();
        }
        throw new AssertionError("expected an InvalidDescriptorException");
    }

    private static CrawlerDescriptor crawler(String type) {
        SourceDescriptor source = new SourceDescriptor("HTTP", "s", Map.of(), "JSON_ARRAY", "", List.of());
        EventMappingDescriptor mapping = new EventMappingDescriptor(NamingConvention.SNAKE_CASE, List.of(), List.of());
        return new CrawlerDescriptor(type, "c", source, mapping);
    }
}
