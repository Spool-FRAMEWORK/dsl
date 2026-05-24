package software.spool.dsl.descriptors.module.crawler.source;

import software.spool.core.port.serde.EnrichmentRule;

import java.util.List;
import java.util.Map;

public record SourceDescriptor(
        String type,
        String id,
        Map<String, String> configuration,
        String mediaType,
        String rootPath,
        List<EnrichmentRule> enrichment
) {}
