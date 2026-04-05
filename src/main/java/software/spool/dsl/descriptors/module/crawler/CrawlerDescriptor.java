package software.spool.dsl.descriptors.module.crawler;

import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

public record CrawlerDescriptor(
        String id,
        SourceDescriptor source,
        EventMappingDescriptor eventMapping
) {
}