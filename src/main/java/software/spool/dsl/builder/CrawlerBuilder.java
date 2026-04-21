package software.spool.dsl.builder;

import software.spool.core.port.serde.NamingConvention;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.crawler.api.Crawler;
import software.spool.crawler.api.builder.CrawlerBuilderFactory;
import software.spool.crawler.api.builder.EventMappingSpecification;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.crawler.api.utils.CrawlerPorts;
import software.spool.crawler.api.utils.StandardNormalizer;
import software.spool.dsl.EventBusFactory;
import software.spool.dsl.InboxWriterFactory;
import software.spool.dsl.SourceFactory;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.PartitionAttributeDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.poll.ScheduleDescriptor;

import java.time.Duration;

public class CrawlerBuilder {
    public static Crawler buildFrom(CrawlerDescriptor crawler, InfrastructureDescriptor infrastructure) {
        return CrawlerBuilderFactory.watchdog(infrastructure.watchdog().url(), crawler.id())
                .poll(buildPollSourceFrom(crawler.source()))
                .schedule(buildScheduleFrom(crawler.source().poll().schedule()))
                .ports(buildPortsFrom(infrastructure))
                .eventMapping(buildEventMappingFrom(crawler.eventMapping()))
                .createWith(new StandardNormalizer.Builder()
                        .rootPath(crawler.source().rootPath())
                        .enrichRules(crawler.source().enrichment())
                        .valueOf(crawler.source().format()));
    };

    private static PollSource<?> buildPollSourceFrom(SourceDescriptor sourceDescriptor) {
        return SourceFactory.pollFrom(sourceDescriptor);
    }

    private static PollingConfiguration buildScheduleFrom(ScheduleDescriptor scheduleDescriptor) {
        return PollingConfiguration.every(Duration.ofMillis(scheduleDescriptor.everyMilliseconds()));
    }

    private static EventMappingSpecification buildEventMappingFrom(EventMappingDescriptor eventMappingDescriptor) {
        return new EventMappingSpecification(NamingConvention.SNAKE_CASE)
                .addPartitionAttributes(eventMappingDescriptor.attributeList().stream()
                        .map(PartitionAttributeDescriptor::value)
                        .toArray(String[]::new));
    }

    private static CrawlerPorts buildPortsFrom(InfrastructureDescriptor infrastructure) {
        return CrawlerPorts.builder()
                .bus(EventBusFactory.from(infrastructure.eventBus()))
                .inbox(InboxWriterFactory.from(infrastructure.inbox()))
                .build();
    }
}
