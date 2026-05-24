package software.spool.dsl.yaml;

import software.spool.core.port.serde.EnrichmentRule;
import software.spool.core.port.serde.NamingConvention;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.yaml.raw.RawComponentDescriptor;
import software.spool.dsl.yaml.raw.RawInfrastructureDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class DescriptorMapper {
    private DescriptorMapper() {}

    public static SpoolNodeDescriptor map(RawSpoolNodeDescriptor raw) {
        return new SpoolNodeDescriptor(
                toInfrastructure(raw.infrastructure()),
                raw.modules().stream()
                        .map(DescriptorMapper::toModule)
                        .toList()
        );
    }

    private static InfrastructureDescriptor toInfrastructure(RawInfrastructureDescriptor raw) {
        return new InfrastructureDescriptor(
                raw.watchdog(),
                toComponent(raw.eventBus()),
                toComponent(raw.inbox()),
                toComponent(raw.dataLake())
        );
    }

    private static InfrastructureComponentDescriptor toComponent(RawComponentDescriptor raw) {
        return new InfrastructureComponentDescriptor(
                raw.type(),
                raw.configuration() != null ? raw.configuration() : Map.of()
        );
    }

    @SuppressWarnings("unchecked")
    private static SpoolModuleDescriptor toModule(Map<String, Object> raw) {
        if (raw.containsKey("crawler"))  return toCrawler((Map<String, Object>) raw.get("crawler"));
        if (raw.containsKey("janitor"))  return toJanitor((Map<String, Object>) raw.get("janitor"));
        if (raw.containsKey("ingester")) return toIngester((Map<String, Object>) raw.get("ingester"));
        throw new IllegalArgumentException("Unknown module type in: " + raw.keySet());
    }

    @SuppressWarnings("unchecked")
    private static CrawlerDescriptor toCrawler(Map<String, Object> raw) {
        Map<String, Object> sourceRaw = (Map<String, Object>) raw.get("source");
        Map<String, Object> mappingRaw = (Map<String, Object>) raw.get("eventMapping");

        return new CrawlerDescriptor(
                (String) raw.get("type"),
                (String) raw.get("id"),
                toSource(sourceRaw),
                toEventMapping(mappingRaw)
        );
    }

    @SuppressWarnings("unchecked")
    private static SourceDescriptor toSource(Map<String, Object> raw) {
        Map<String, String> config = toStringMap(
                (Map<String, Object>) raw.getOrDefault("configuration", Map.of())
        );
        List<EnrichmentRule> enrichment = toEnrichmentRules(
                (List<Map<String, String>>) raw.getOrDefault("enrichment", List.of()));
        return new SourceDescriptor(
                (String) raw.get("type"),
                (String) raw.get("id"),
                config,
                (String) raw.get("mediaType"),
                (String) raw.get("rootPath"),
                enrichment
        );
    }

    @SuppressWarnings("unchecked")
    private static EventMappingDescriptor toEventMapping(Map<String, Object> raw) {
        List<Object> rawAttributes = (List<Object>) raw.getOrDefault("attributeList", List.of());
        List<String> attributes = rawAttributes.stream()
                .map(entry -> entry instanceof Map<?, ?> m
                        ? (String) m.get("value")
                        : (String) entry)
                .filter(Objects::nonNull)
                .toList();
        List<Object> rawDomainMappings = (List<Object>) raw.getOrDefault("domainMappingList", List.of());
        List<String> domainMappings = rawDomainMappings.stream()
                .map(entry -> entry instanceof Map<?, ?> m
                        ? (String) m.get("value")
                        : (String) entry)
                .filter(Objects::nonNull)
                .toList();
        return new EventMappingDescriptor(
                NamingConvention.valueOf((String) raw.get("namingConvention")),
                domainMappings,
                attributes
        );
    }

    private static List<EnrichmentRule> toEnrichmentRules(List<Map<String, String>> raw) {
        return raw.stream()
                .map(r -> new EnrichmentRule(r.get("source"), r.get("target")))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static JanitorDescriptor toJanitor(Map<String, Object> raw) {
        return new JanitorDescriptor(
                (String) raw.get("id"),
                toStringMap((Map<String, Object>) raw.getOrDefault("configuration", Map.of()))
        );
    }

    @SuppressWarnings("unchecked")
    private static IngesterDescriptor toIngester(Map<String, Object> raw) {
        return new IngesterDescriptor(
                (String) raw.get("type"),
                (String) raw.get("id"),
                toStringMap((Map<String, Object>) raw.getOrDefault("configuration", Map.of()))
        );
    }

    private static Map<String, String> toStringMap(Map<String, Object> raw) {
        if (raw == null) return Map.of();
        return raw.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> String.valueOf(e.getValue())
                ));
    }
}