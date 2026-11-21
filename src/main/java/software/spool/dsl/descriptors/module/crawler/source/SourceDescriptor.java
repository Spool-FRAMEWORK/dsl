package software.spool.dsl.descriptors.module.crawler.source;

import software.spool.core.model.vo.MediaType;
import software.spool.core.port.serde.EnrichmentRule;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Where a crawler reads from, which normalizer turns what it reads into records and the media type those records
 * carry. The normalizer and the media type are declared apart: the media type only labels the records.
 */
public record SourceDescriptor(
        String type,
        String id,
        Map<String, String> configuration,
        NormalizerDescriptor normalizer,
        MediaType mediaType,
        String rootPath,
        List<EnrichmentRule> enrichment
) {
    public SourceDescriptor {
        rootPath = Objects.requireNonNullElse(rootPath, "");
    }
}
