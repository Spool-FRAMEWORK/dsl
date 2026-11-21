package software.spool.dsl.providers;

import software.spool.core.port.quarantine.QuarantineRepository;
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
import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxUpdaterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.quarantine.QuarantineRepositoryProvider;
import software.spool.core.port.datalake.DataLakeWriter;

/**
 * Builds the infrastructure ports a module asks for. A component is required only when a module asks
 * for it, so a node with just a crawler does not need a data lake in its descriptor.
 *
 * <p>The backend of each port is chosen by name: the descriptor says which one, as in {@code type: S3}, and this
 * factory looks it up with {@link KnownPlugins}. {@code supports()} and {@code priority()} play no part in that. They
 * only decide where nothing names the plugin: which {@code SpoolModuleProvider} builds a module, and the event bus
 * behind an in-memory stream source.</p>
 *
 * <p>To replace a framework provider with your own, register it with the same name and a lower priority number than
 * the framework's, and descriptors keep working unchanged. The framework uses 10, except the file system inbox
 * providers, which use 0, and the in-memory event bus, which uses 100, so replacing those takes a number below
 * theirs. Two providers with the same name and priority fail when they are registered, naming both classes.</p>
 */
public final class InfrastructurePluginFactory {

    private InfrastructurePluginFactory() {}

    public static EventBus eventBus(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor bus = required(infra.eventBus(), "eventBus");
        return KnownPlugins.get(EventBusProvider.class, bus.pluginName(), "infrastructure.eventBus.type", "event bus")
                .create(bus.toPluginConfiguration());
    }

    public static EventPublisher tracedEventPublisher(InfrastructureDescriptor infra) {
        return TraceEventPublisher.of(eventBus(infra))
                .with(new OpenTelemetryTracedEventBus());
    }

    public static InboxWriter inboxWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return KnownPlugins.get(InboxWriterProvider.class, inbox.pluginName(), "infrastructure.inbox.type", "inbox writer")
                .create(inbox.toPluginConfiguration());
    }

    public static InboxReader inboxReader(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return KnownPlugins.get(InboxReaderProvider.class, inbox.pluginName(), "infrastructure.inbox.type", "inbox reader")
                .create(inbox.toPluginConfiguration());
    }

    public static InboxUpdater inboxUpdater(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return KnownPlugins.get(InboxUpdaterProvider.class, inbox.pluginName(), "infrastructure.inbox.type", "inbox updater")
                .create(inbox.toPluginConfiguration());
    }

    public static InboxEnvelopeRemover inboxEnvelopeRemover(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = required(infra.inbox(), "inbox");
        return KnownPlugins.get(InboxEnvelopeRemoverProvider.class, inbox.pluginName(), "infrastructure.inbox.type", "inbox envelope remover")
                .create(inbox.toPluginConfiguration());
    }

    public static DataLakeWriter dataLakeWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = required(infra.dataLake(), "dataLake");
        return KnownPlugins.get(DataLakeWriterProvider.class, dataLake.pluginName(), "infrastructure.dataLake.type", "data lake writer")
                .create(dataLake.toPluginConfiguration());
    }

    public static DataLakeEnvelopeRemover dataLakeEnvelopeRemover(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = required(infra.dataLake(), "dataLake");
        return KnownPlugins.get(DataLakeEnvelopeRemoverProvider.class, dataLake.pluginName(), "infrastructure.dataLake.type", "data lake envelope remover")
                .create(dataLake.toPluginConfiguration());
    }

    public static PartitionedReader partitionedReader(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = required(infra.dataLake(), "dataLake");
        return KnownPlugins.get(PartitionedReaderProvider.class, dataLake.pluginName(), "infrastructure.dataLake.type", "partitioned reader")
                .create(dataLake.toPluginConfiguration());
    }

    public static DataMartWriter dataMartWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataMart = required(infra.dataMart(), "dataMart");
        return KnownPlugins.get(DataMartWriterProvider.class, dataMart.pluginName(), "infrastructure.dataMart.type", "data mart writer")
                .create(dataMart.toPluginConfiguration());
    }

    public static DataMartWriter dataLakeMountWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = required(infra.dataLake(), "dataLake");
        return KnownPlugins.get(DataMartWriterProvider.class, dataLake.pluginName(), "infrastructure.dataLake.type", "data lake mount writer")
                .create(dataLake.toPluginConfiguration());
    }

    public static QuarantineRepository quarantineRepository(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor quarantineRepository = required(infra.quarantineRepository(), "quarantineRepository");
        return KnownPlugins.get(QuarantineRepositoryProvider.class, quarantineRepository.pluginName(), "infrastructure.quarantineRepository.type", "quarantine repository")
                .create(quarantineRepository.toPluginConfiguration());
    }

    public static CrawlerPorts crawlerPorts(InfrastructureDescriptor infra) {
        return CrawlerPorts.builder()
                .bus(tracedEventPublisher(infra))
                .inbox(inboxWriter(infra))
                .build();
    }

    public static Normalizer<?> normalizer(SourceDescriptor source) {
        return NormalizerComposer.withDeclaredSteps().compose(source.normalizer(), PluginConfiguration.builder()
                .with("rules", new String(RecordSerializerFactory.record().serialize(source.enrichment())))
                .with("rootPath", source.rootPath())
                .build());
    }

    private static InfrastructureComponentDescriptor required(InfrastructureComponentDescriptor component, String key) {
        return DescriptorReader.require("infrastructure." + key, component);
    }
}
