package software.spool.dsl.providers;

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
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
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

// software.spool.infrastructure.adapter.spool.InfrastructurePluginFactory
public final class InfrastructurePluginFactory {

    private InfrastructurePluginFactory() {}

    // ── Event Bus ──────────────────────────────────────────────────────────

    public static EventBus eventBus(InfrastructureDescriptor infra) {
        return PluginResolver.get(EventBusProvider.class, infra.eventBus().pluginName())
                .create(infra.eventBus().toPluginConfiguration());
    }

    public static EventPublisher tracedEventPublisher(InfrastructureDescriptor infra) {
        return TraceEventPublisher.of(eventBus(infra))
                .with(new OpenTelemetryTracedEventBus());
    }

    // ── Inbox ──────────────────────────────────────────────────────────────

    public static InboxWriter inboxWriter(InfrastructureDescriptor infra) {
        return PluginResolver.get(InboxWriterProvider.class, infra.inbox().pluginName())
                .create(infra.inbox().toPluginConfiguration());
    }

    public static InboxReader inboxReader(InfrastructureDescriptor infra) {
        return PluginResolver.get(InboxReaderProvider.class, infra.inbox().pluginName())
                .create(infra.inbox().toPluginConfiguration());
    }

    public static InboxUpdater inboxUpdater(InfrastructureDescriptor infra) {
        return PluginResolver.get(InboxUpdaterProvider.class, infra.inbox().pluginName())
                .create(infra.inbox().toPluginConfiguration());
    }

    public static InboxEnvelopeRemover inboxEnvelopeRemover(InfrastructureDescriptor infra) {
        return PluginResolver.get(InboxEnvelopeRemoverProvider.class, infra.inbox().pluginName())
                .create(infra.inbox().toPluginConfiguration());
    }

    // ── DataLake ───────────────────────────────────────────────────────────

    public static DataLakeWriter dataLakeWriter(InfrastructureDescriptor infra) {
        return PluginResolver.get(DataLakeWriterProvider.class, infra.dataLake().pluginName())
                .create(infra.dataLake().toPluginConfiguration());
    }

    // ── Puertos compuestos por módulo ──────────────────────────────────────

    public static CrawlerPorts crawlerPorts(InfrastructureDescriptor infra) {
        return CrawlerPorts.builder()
                .bus(tracedEventPublisher(infra))
                .inbox(inboxWriter(infra))
                .build();
    }

    // ── Normalizer ─────────────────────────────────────────────────────────

    public static Normalizer<?, ?, ?> normalizer(SourceDescriptor source) {
        return PluginResolver.get(NormalizerProvider.class,
                        source.mediaType().toUpperCase() + "_NORMALIZER")
                .create(PluginConfiguration.builder()
                        .with("rules",    source.enrichment().toString())
                        .with("rootPath", source.rootPath())
                        .build());
    }
}