package software.spool.dsl.builder;

import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.port.decorator.TraceEventPublisher;
import software.spool.core.port.serde.NamingConvention;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.crawler.api.Crawler;
import software.spool.crawler.api.builder.CrawlerBuilderFactory;
import software.spool.crawler.api.builder.EventMappingSpecification;
import software.spool.crawler.api.port.InboxWriter;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.crawler.api.utils.CrawlerPorts;
import software.spool.crawler.api.utils.StandardNormalizer;
import software.spool.dsl.SourceFactory;
import software.spool.dsl.descriptors.infrastructure.EventBusDescriptor;
import software.spool.dsl.descriptors.infrastructure.InboxDescriptor;
import software.spool.dsl.descriptors.infrastructure.InboxType;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.PartitionAttributeDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.poll.PollSourceType;
import software.spool.dsl.descriptors.module.crawler.source.poll.ScheduleDescriptor;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.EventBusProvider;
import software.spool.infrastructure.spi.provider.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.PollSourceProvider;

import java.time.Duration;
import java.util.Map;

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
        return sourceDescriptor.poll().type() == PollSourceType.CUSTOM ?
                PluginResolver.get(PollSourceProvider.class, sourceDescriptor.poll().custom().pluginName())
                                .create(buildConfigurationFrom(sourceDescriptor)) :
                SourceFactory.pollFrom(sourceDescriptor);
    }

    private static PluginConfiguration buildConfigurationFrom(SourceDescriptor sourceDescriptor) {
        Map<String, String> configuration = sourceDescriptor.poll().custom().configuration();
        configuration.put("sourceId", sourceDescriptor.id());
        return buildConfigurationFrom(configuration);
    }

    private static PluginConfiguration buildConfigurationFrom(Map<String, String> configuration) {
        PluginConfiguration.Builder builder = PluginConfiguration.builder();
        configuration.forEach(builder::with);
        return builder.build();
    }

    private static PollingConfiguration buildScheduleFrom(ScheduleDescriptor scheduleDescriptor) {
        return scheduleDescriptor.everyMilliseconds() > 1000 ? PollingConfiguration.every(Duration.ofMillis(scheduleDescriptor.everyMilliseconds())) :
                PollingConfiguration.once();
    }

    private static EventMappingSpecification buildEventMappingFrom(EventMappingDescriptor eventMappingDescriptor) {
        return new EventMappingSpecification(NamingConvention.SNAKE_CASE)
                .addPartitionAttributes(eventMappingDescriptor.attributeList().stream()
                        .map(PartitionAttributeDescriptor::value)
                        .toArray(String[]::new));
    }

    private static CrawlerPorts buildPortsFrom(InfrastructureDescriptor infrastructure) {
        return CrawlerPorts.builder()
                .bus(TraceEventPublisher.of(PluginResolver.get(EventBusProvider.class, infrastructure.eventBus().type().name().toUpperCase()).create(buildBusConfigurationFrom(infrastructure.eventBus()))).with(new OpenTelemetryTracedEventBus()))
                .inbox(getInboxPluginFrom(infrastructure.inbox()))
                .build();
    }

    private static InboxWriter getInboxPluginFrom(InboxDescriptor inboxDescriptor) {
        return inboxDescriptor.type() == InboxType.CUSTOM ?
                PluginResolver.get(InboxWriterProvider.class, inboxDescriptor.custom().pluginName())
                        .create(buildConfigurationFrom(inboxDescriptor.custom().configuration())) :
                PluginResolver.get(InboxWriterProvider.class, inboxDescriptor.type().name().toUpperCase())
                        .create(buildConfigurationFrom(inboxDescriptor.custom().configuration()));
    }

    private static PluginConfiguration buildInboxConfigurationFrom(InboxDescriptor inboxDescriptor) {
        return PluginConfiguration.builder()
                .with("endpoint", inboxDescriptor.s3().endpoint())
                .with("region", inboxDescriptor.s3().region())
                .with("bucket", inboxDescriptor.s3().bucket()).build();
    }

    private static PluginConfiguration buildBusConfigurationFrom(EventBusDescriptor eventBusDescriptor) {
        return PluginConfiguration.builder()
                .with("bootstrap.servers", eventBusDescriptor.url()).build();
    }
}
