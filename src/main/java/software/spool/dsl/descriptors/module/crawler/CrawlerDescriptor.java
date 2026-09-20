package software.spool.dsl.descriptors.module.crawler;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

/**
 * A crawler module. {@code errorRouter} is optional and is {@code null} when the descriptor does not name one,
 * in which case the crawler uses its default router.
 */
public record CrawlerDescriptor(
        String type,
        String id,
        SourceDescriptor source,
        EventMappingDescriptor eventMapping,
        ErrorRouterDescriptor errorRouter
) implements SpoolModuleDescriptor {

    public CrawlerDescriptor(String type, String id, SourceDescriptor source, EventMappingDescriptor eventMapping) {
        this(type, id, source, eventMapping, null);
    }

    @Override
    public String moduleType() {
        return "CRAWLER_" + type;
    }
}