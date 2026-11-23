package software.spool.dsl.descriptors.module.crawler.source;

import software.spool.core.model.vo.IdempotencyKeySchema;
import software.spool.core.model.vo.MediaType;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.core.resilience.model.CircuitBreakerPolicy;

import software.spool.crawler.api.port.WhenQuarantined;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Where a crawler reads from, which normalizer turns what it reads into records and the media type those records
 * carry. The normalizer and the media type are declared apart: the media type only labels the records. The circuit
 * says when the source stops being called after failing, and the idempotency key schema which fields identify a
 * record, the whole record when the source names none.
 */
public record SourceDescriptor(
        String type,
        String id,
        Map<String, String> configuration,
        NormalizerDescriptor normalizer,
        MediaType mediaType,
        String rootPath,
        List<EnrichmentRule> enrichment,
        CircuitBreakerPolicy circuit,
        IdempotencyKeySchema idempotencyKeySchema,
        WhenQuarantined whenQuarantined
) {
    public SourceDescriptor {
        rootPath = Objects.requireNonNullElse(rootPath, "");
        idempotencyKeySchema = Objects.requireNonNullElse(idempotencyKeySchema, IdempotencyKeySchema.wholeRecord());
        whenQuarantined = Objects.requireNonNullElse(whenQuarantined, WhenQuarantined.KEEP);
    }

    public SourceDescriptor(String type, String id, Map<String, String> configuration, NormalizerDescriptor normalizer,
                            MediaType mediaType, String rootPath, List<EnrichmentRule> enrichment) {
        this(type, id, configuration, normalizer, mediaType, rootPath, enrichment, CircuitBreakerPolicy.defaults(),
                IdempotencyKeySchema.wholeRecord(), WhenQuarantined.KEEP);
    }
}
