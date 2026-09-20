package software.spool.dsl.providers;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.bus.EventPublisher;
import software.spool.core.port.decorator.TraceEventPublisher;
import software.spool.core.port.inbox.InboxEnvelopeRemover;
import software.spool.core.port.inbox.InboxReader;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.crawler.api.port.InboxWriter;
import software.spool.crawler.api.utils.CrawlerPorts;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxUpdaterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;
import software.spool.ingester.api.port.DataLakeWriter;

/**
 * Builds the infrastructure ports a module asks for. A component is required only when a module asks
 * for it, so a node with just a crawler does not need a data lake in its descriptor.
 */
public final class InfrastructurePluginFactory {

    private InfrastructurePluginFactory() {}

    public static EventBus eventBus(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor bus = required(infra.eventBus(), "eventBus");
        return PluginResolver.get(EventBusProvider.class, bus.pluginName())
                .create(bus.toPluginConfiguration());
    }

    public static EventPublisher tracedEventPublisher(InfrastructureDescriptor infra) {
        return TraceEventPublisher.of(eventBus(infra))
                .with(new OpenTelemetryTracedEventBus());
    }

    public static InboxWriter inboxWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return PluginResolver.get(InboxWriterProvider.class, inbox.pluginName())
                .create(inbox.toPluginConfiguration());
    }

    public static InboxReader inboxReader(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return PluginResolver.get(InboxReaderProvider.class, inbox.pluginName())
                .create(inbox.toPluginConfiguration());
    }

    public static InboxUpdater inboxUpdater(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return PluginResolver.get(InboxUpdaterProvider.class, inbox.pluginName())
                .create(inbox.toPluginConfiguration());
    }

    public static InboxEnvelopeRemover inboxEnvelopeRemover(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return PluginResolver.get(InboxEnvelopeRemoverProvider.class, inbox.pluginName())
                .create(inbox.toPluginConfiguration());
    }

    public static DataLakeWriter dataLakeWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = required(infra.dataLake(), "dataLake");
        return PluginResolver.get(DataLakeWriterProvider.class, dataLake.pluginName())
                .create(dataLake.toPluginConfiguration());
    }

    public static CrawlerPorts crawlerPorts(InfrastructureDescriptor infra) {
        return CrawlerPorts.builder()
                .bus(tracedEventPublisher(infra))
                .inbox(inboxWriter(infra))
                .build();
    }

    public static Normalizer<?> normalizer(SourceDescriptor source) {
        return PluginResolver.get(NormalizerProvider.class,
                        source.mediaType().toUpperCase() + "_NORMALIZER")
                .create(PluginConfiguration.builder()
                        .with("rules", new String(RecordSerializerFactory.record().serialize(source.enrichment())))
                        .with("rootPath", source.rootPath())
                        .build());
    }

    private static InfrastructureComponentDescriptor required(InfrastructureComponentDescriptor component, String key) {
        return DescriptorReader.require("infrastructure." + key, component);
    }
}
