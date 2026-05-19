package software.spool.dsl.builder;

import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.decorator.TraceEventPublisher;
import software.spool.core.port.serde.NamingConvention;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.crawler.api.Crawler;
import software.spool.crawler.api.builder.CrawlerBuilderFactory;
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
        SourceDescriptor source = crawler.source();
        return CrawlerBuilderFactory.watchdog(infrastructure.watchdog().url(), crawler.id()).poll(buildPollSourceFrom(source))
                .source()
                    .schedule(buildScheduleFrom(source.poll().schedule()))
                    .ports(buildPortsFrom(infrastructure))
                    .enrichRules(source.enrichment())
                    .rootPath(source.rootPath())
                    .mediaType(source.mediaType())
                    .and()
                .mapping()
                    .convention(NamingConvention.SNAKE_CASE)
                    .addPartitionAttributes(toPartitionAttributeArray(crawler.eventMapping()))
                    .and()
                .createWith(new StandardNormalizer.Builder()
                        .rootPath(source.rootPath())
                        .enrichRules(source.enrichment())
                        .valueOf(source.format()));
    }

    private static PollSource<?> buildPollSourceFrom(SourceDescriptor source) {
        if (source.poll().type() == PollSourceType.CUSTOM) {
            return PluginResolver.get(PollSourceProvider.class, source.poll().custom().pluginName())
                    .create(buildCustomSourceConfiguration(source));
        }
        return SourceFactory.pollFrom(source);
    }

    private static PluginConfiguration buildCustomSourceConfiguration(SourceDescriptor source) {
        Map<String, String> config = source.poll().custom().configuration();
        config.put("sourceId", source.id());
        return toPluginConfiguration(config);
    }

    // --- Schedule ---

    private static PollingConfiguration buildScheduleFrom(ScheduleDescriptor schedule) {
        return schedule.everyMilliseconds() > 1000
                ? PollingConfiguration.every(Duration.ofMillis(schedule.everyMilliseconds()))
                : PollingConfiguration.once();
    }

    // --- Ports ---

    private static CrawlerPorts buildPortsFrom(InfrastructureDescriptor infrastructure) {
        return CrawlerPorts.builder()
                .bus(buildTracedEventBus(infrastructure.eventBus()))
                .inbox(buildInboxWriter(infrastructure.inbox()))
                .build();
    }

    private static EventPublisher buildTracedEventBus(EventBusDescriptor eventBus) {
        PluginConfiguration config = PluginConfiguration.builder()
                .with("bootstrap.servers", eventBus.url())
                .build();
        return TraceEventPublisher.of(
                PluginResolver.get(EventBusProvider.class, eventBus.type().name().toUpperCase())
                        .create(config)
        ).with(new OpenTelemetryTracedEventBus());
    }

    private static InboxWriter buildInboxWriter(InboxDescriptor inbox) {
        String pluginName = inbox.type() == InboxType.CUSTOM
                ? inbox.custom().pluginName()
                : inbox.type().name().toUpperCase();
        return PluginResolver.get(InboxWriterProvider.class, pluginName)
                .create(toPluginConfiguration(inbox.custom().configuration()));
    }

    // --- Shared utility ---

    private static PluginConfiguration toPluginConfiguration(Map<String, String> configuration) {
        PluginConfiguration.Builder builder = PluginConfiguration.builder();
        configuration.forEach(builder::with);
        return builder.build();
    }

    private static String[] toPartitionAttributeArray(EventMappingDescriptor eventMapping) {
        return eventMapping.attributeList().stream()
                .map(PartitionAttributeDescriptor::value)
                .toArray(String[]::new);
    }
}