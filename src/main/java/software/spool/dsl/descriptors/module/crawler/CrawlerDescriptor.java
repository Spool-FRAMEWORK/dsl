package software.spool.dsl.descriptors.module.crawler;

import software.spool.core.model.schedule.Schedule;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

/**
 * A crawler module. {@code errorRouter} is optional and is {@code null} when the descriptor does not name one,
 * in which case the crawler uses its default router. A polling crawler polls on its schedule, which is once when
 * the descriptor does not declare one; a streaming crawler has no rounds and ignores it.
 */
public record CrawlerDescriptor(
        String type,
        String id,
        SourceDescriptor source,
        EventMappingDescriptor eventMapping,
        ErrorRouterDescriptor errorRouter,
        Schedule schedule
) implements SpoolModuleDescriptor {

    public CrawlerDescriptor(String type, String id, SourceDescriptor source, EventMappingDescriptor eventMapping) {
        this(type, id, source, eventMapping, null, Schedule.once());
    }

    @Override
    public String moduleType() {
        return "CRAWLER_" + type;
    }
}