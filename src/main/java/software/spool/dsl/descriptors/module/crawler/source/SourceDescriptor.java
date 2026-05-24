package software.spool.dsl.descriptors.module.crawler.source;

import software.spool.core.port.serde.EnrichmentRule;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record SourceDescriptor(
        String type,
        String id,
        Map<String, String> configuration,
        String mediaType,
        String rootPath,
        List<EnrichmentRule> enrichment
) {
    public SourceDescriptor(String type, String id, Map<String, String> configuration, String mediaType, String rootPath, List<EnrichmentRule> enrichment) {
        this.type = type;
        this.id = id;
        this.configuration = configuration;
        this.mediaType = mediaType;
        this.rootPath = Objects.requireNonNullElse(rootPath, "");
        this.enrichment = enrichment;
    }
}
