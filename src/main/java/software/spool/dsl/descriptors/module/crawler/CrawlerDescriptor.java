package software.spool.dsl.descriptors.module.crawler;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

public record CrawlerDescriptor(
        String type,
        String id,
        SourceDescriptor source,
        EventMappingDescriptor eventMapping
) implements SpoolModuleDescriptor {
    @Override
    public String moduleType() {
        return "CRAWLER_" + type;
    }
}