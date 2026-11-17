package software.spool.dsl.descriptors.module.crawler;

import software.spool.dsl.descriptors.module.ScheduleDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

import java.util.Optional;

/**
 * A crawler module. {@code errorRouter} is optional and is {@code null} when the descriptor does not name one,
 * in which case the crawler uses its default router. Without a schedule, a polling crawler polls once.
 */
public record CrawlerDescriptor(
        String type,
        String id,
        SourceDescriptor source,
        EventMappingDescriptor eventMapping,
        ErrorRouterDescriptor errorRouter,
        Optional<ScheduleDescriptor> schedule
) implements SpoolModuleDescriptor {

    public CrawlerDescriptor(String type, String id, SourceDescriptor source, EventMappingDescriptor eventMapping) {
        this(type, id, source, eventMapping, null, Optional.empty());
    }

    @Override
    public String moduleType() {
        return "CRAWLER_" + type;
    }
}