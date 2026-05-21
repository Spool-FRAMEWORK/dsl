package software.spool.dsl.descriptors.module.crawler.source;

import software.spool.core.port.serde.EnrichmentRule;
import software.spool.crawler.api.utils.StandardNormalizer;
import software.spool.dsl.descriptors.module.crawler.source.poll.PollSourceDescriptor;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public record SourceDescriptor(
    String id,
    StandardNormalizer.Format format,
    String rootPath,
    String mediaType,
    List<EnrichmentRule> enrichment,
    PollSourceDescriptor poll
) {
    public SourceDescriptor(String id, StandardNormalizer.Format format, String rootPath, String mediaType, List<EnrichmentRule> enrichment, PollSourceDescriptor poll) {
        this.id = id;
        this.format = format;
        this.rootPath = rootPath;
        this.mediaType = mediaType;
        this.enrichment = Objects.requireNonNullElse(enrichment, List.of());
        this.poll = poll;
    }

    public SourceType type() {
        String fieldName = Arrays.stream(getClass().getRecordComponents())
                .filter(component -> valueOf(component) != null)
                .map(RecordComponent::getName)
                .filter(name -> !name.startsWith("mediaType") && !name.startsWith("id") && !name.startsWith("format") && !name.startsWith("rootPath") && !name.startsWith("enrichment"))
                .findFirst()
                .orElseThrow();
        return SourceType.fromFieldName(fieldName);
    }

    private Object valueOf(RecordComponent component) {
        try {
            return component.getAccessor().invoke(this);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot inspect source descriptor", e);
        }
    }
}
