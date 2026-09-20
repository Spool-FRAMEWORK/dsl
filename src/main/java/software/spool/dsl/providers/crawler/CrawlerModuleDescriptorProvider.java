package software.spool.dsl.providers.crawler;

import software.spool.core.port.serde.EnrichmentRule;
import software.spool.core.port.serde.NamingConvention;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParser;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class CrawlerModuleDescriptorProvider implements ModuleDescriptorProvider {

    /** A list whose entries are plain texts or objects with a {@code value}; the YAML accepts both. */
    private static final ValueParser<List<String>> TEXTS = ValueParsers.of("a list of texts", raw -> {
        if (!(raw instanceof List<?> entries)) return Optional.empty();
        List<String> texts = new ArrayList<>();
        for (Object entry : entries) {
            Object value = entry instanceof Map<?, ?> object ? object.get("value") : entry;
            if (value == null) continue;
            Optional<String> text = ValueParsers.STRING.parse(value);
            if (text.isEmpty()) return Optional.empty();
            texts.add(text.get());
        }
        return Optional.of(texts);
    });

    @Override
    public String name() {
        return "CRAWLER";
    }

    @Override
    public SpoolModuleDescriptor read(DescriptorReader crawler) {
        return new CrawlerDescriptor(
                crawler.string("type"),
                crawler.string("id"),
                readSource(crawler.object("source")),
                readEventMapping(crawler.object("eventMapping"))
        );
    }

    private static SourceDescriptor readSource(DescriptorReader source) {
        source.objectOrEmpty("configuration").optional("scheduleMilliseconds", ValueParsers.INTEGER);
        return new SourceDescriptor(
                source.string("type"),
                source.optional("id", ValueParsers.STRING).orElse(null),
                source.stringMap("configuration"),
                source.string("mediaType"),
                source.optional("rootPath", ValueParsers.STRING).orElse(null),
                source.list("enrichment").stream().map(CrawlerModuleDescriptorProvider::readRule).toList()
        );
    }

    private static EnrichmentRule readRule(DescriptorReader rule) {
        return new EnrichmentRule(rule.string("source"), rule.optional("target", ValueParsers.STRING).orElse(null));
    }

    private static EventMappingDescriptor readEventMapping(DescriptorReader mapping) {
        return new EventMappingDescriptor(
                mapping.required("namingConvention", ValueParsers.oneOf(NamingConvention.class)),
                mapping.optional("domainMappingList", TEXTS).orElse(List.of()),
                mapping.optional("attributeList", TEXTS).orElse(List.of())
        );
    }
}
