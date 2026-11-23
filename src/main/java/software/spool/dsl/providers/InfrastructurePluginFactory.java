package software.spool.dsl.providers;

import software.spool.core.adapter.health.MonitoredDataLakeEnvelopeRemover;
import software.spool.core.adapter.health.MonitoredDataLakeWriter;
import software.spool.core.adapter.health.MonitoredEnvelopeHeaderRemover;
import software.spool.core.adapter.health.MonitoredEnvelopeHeaderWriter;
import software.spool.core.adapter.health.MonitoredEventBus;
import software.spool.core.adapter.health.MonitoredInboxEnvelopeRemover;
import software.spool.core.adapter.health.MonitoredInboxReader;
import software.spool.core.adapter.health.MonitoredInboxUpdater;
import software.spool.core.adapter.health.MonitoredQuarantineRepository;
import software.spool.core.port.health.PieceMonitor;
import software.spool.core.port.quarantine.QuarantineRepository;
import software.spool.crawler.api.adapter.health.MonitoredInboxWriter;
import software.spool.crawler.api.adapter.health.MonitoredPollSource;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.mounter.api.adapter.health.MonitoredAggregateMetadataRepository;
import software.spool.mounter.api.adapter.health.MonitoredDataMartRemover;
import software.spool.mounter.api.adapter.health.MonitoredDataMartWriter;
import software.spool.mounter.api.adapter.health.MonitoredPartitionDiscovery;
import software.spool.mounter.api.adapter.health.MonitoredPartitionedReader;
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
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.PollSourceProvider;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartRemoverProvider;
import software.spool.infrastructure.spi.provider.aggregate.AggregateMetadataRepositoryProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionDiscoveryProvider;
import software.spool.mounter.api.MountMode;
import software.spool.mounter.api.port.AggregateMetadataRepository;
import software.spool.mounter.api.port.DataMartRemover;
import software.spool.mounter.api.port.scaling.PartitionDiscovery;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.PartitionedReader;
import software.spool.core.port.datalake.DataLakeEnvelopeRemover;
import software.spool.core.port.datalake.EnvelopeHeaderRemover;
import software.spool.core.port.datalake.EnvelopeHeaderWriter;
import software.spool.core.utils.datalake.IndexedDataLakeEnvelopeRemover;
import software.spool.core.utils.datalake.IndexedDataLakeWriter;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderRemoverProvider;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderWriterProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxUpdaterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.quarantine.QuarantineRepositoryProvider;
import software.spool.core.port.datalake.DataLakeWriter;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Builds the infrastructure ports a module asks for, from the pieces its infrastructure resolves for it. A piece is
 * required only when a module asks for it, so a node with just a crawler does not need a data lake in its descriptor.
 * Each port is watched by the monitor of its piece, so that the health of the module tells how the piece is doing.
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
        InfrastructureComponentDescriptor bus = infra.eventBus();
        return new MonitoredEventBus(KnownPlugins.get(EventBusProvider.class, bus.pluginName(), bus.path() + ".type", "event bus")
                .create(bus.toPluginConfiguration()), monitorOf(infra, bus));
    }

    public static EventPublisher tracedEventPublisher(InfrastructureDescriptor infra) {
        return TraceEventPublisher.of(eventBus(infra))
                .with(new OpenTelemetryTracedEventBus());
    }

    public static InboxWriter inboxWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = infra.inbox();
        return new MonitoredInboxWriter(KnownPlugins.get(InboxWriterProvider.class, inbox.pluginName(), inbox.path() + ".type", "inbox writer")
                .create(inbox.toPluginConfiguration()), monitorOf(infra, inbox));
    }

    public static InboxReader inboxReader(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = infra.inbox();
        return new MonitoredInboxReader(KnownPlugins.get(InboxReaderProvider.class, inbox.pluginName(), inbox.path() + ".type", "inbox reader")
                .create(inbox.toPluginConfiguration()), monitorOf(infra, inbox));
    }

    public static InboxUpdater inboxUpdater(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = infra.inbox();
        return new MonitoredInboxUpdater(KnownPlugins.get(InboxUpdaterProvider.class, inbox.pluginName(), inbox.path() + ".type", "inbox updater")
                .create(inbox.toPluginConfiguration()), monitorOf(infra, inbox));
    }

    public static InboxEnvelopeRemover inboxEnvelopeRemover(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor inbox = infra.inbox();
        return new MonitoredInboxEnvelopeRemover(KnownPlugins.get(InboxEnvelopeRemoverProvider.class, inbox.pluginName(), inbox.path() + ".type", "inbox envelope remover")
                .create(inbox.toPluginConfiguration()), monitorOf(infra, inbox));
    }

    /** Writes to the data lake, and keeps the header of each envelope when the infrastructure has a store for them. */
    public static DataLakeWriter dataLakeWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = infra.dataLake();
        DataLakeWriter writer = new MonitoredDataLakeWriter(KnownPlugins.get(DataLakeWriterProvider.class, dataLake.pluginName(), dataLake.path() + ".type", "data lake writer")
                .create(dataLake.toPluginConfiguration()), monitorOf(infra, dataLake));
        return envelopeHeaders(infra, EnvelopeHeaderWriterProvider.class, "envelope header writer", MonitoredEnvelopeHeaderWriter::new)
                .<DataLakeWriter>map(headers -> new IndexedDataLakeWriter(writer, headers))
                .orElse(writer);
    }

    /** Removes from the data lake, and the header of the envelope too when the infrastructure has a store for them. */
    public static DataLakeEnvelopeRemover dataLakeEnvelopeRemover(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = infra.dataLake();
        DataLakeEnvelopeRemover remover = new MonitoredDataLakeEnvelopeRemover(KnownPlugins.get(DataLakeEnvelopeRemoverProvider.class, dataLake.pluginName(), dataLake.path() + ".type", "data lake envelope remover")
                .create(dataLake.toPluginConfiguration()), monitorOf(infra, dataLake));
        return envelopeHeaders(infra, EnvelopeHeaderRemoverProvider.class, "envelope header remover", MonitoredEnvelopeHeaderRemover::new)
                .<DataLakeEnvelopeRemover>map(headers -> new IndexedDataLakeEnvelopeRemover(headers, remover))
                .orElse(remover);
    }

    private static <T> Optional<T> envelopeHeaders(InfrastructureDescriptor infra, Class<? extends Plugin<T>> provider,
                                                   String label, BiFunction<T, PieceMonitor, T> monitored) {
        return infra.envelopeHeaders().map(piece -> monitored.apply(KnownPlugins.get(provider, piece.pluginName(), piece.path() + ".type", label)
                .create(piece.toPluginConfiguration()), monitorOf(infra, piece)));
    }

    public static PartitionedReader partitionedReader(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = infra.dataLake();
        return new MonitoredPartitionedReader(KnownPlugins.get(PartitionedReaderProvider.class, dataLake.pluginName(), dataLake.path() + ".type", "partitioned reader")
                .create(dataLake.toPluginConfiguration()), monitorOf(infra, dataLake));
    }

    public static DataMartWriter dataMartWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataMart = infra.dataMart();
        return new MonitoredDataMartWriter(KnownPlugins.get(DataMartWriterProvider.class, dataMart.pluginName(), dataMart.path() + ".type", "data mart writer")
                .create(dataMart.toPluginConfiguration()), monitorOf(infra, dataMart));
    }

    public static DataMartWriter dataLakeMountWriter(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataLake = infra.dataLake();
        return new MonitoredDataMartWriter(KnownPlugins.get(DataMartWriterProvider.class, dataLake.pluginName(), dataLake.path() + ".type", "data lake mount writer")
                .create(dataLake.toPluginConfiguration()), monitorOf(infra, dataLake));
    }

    public static DataMartRemover dataMartRemover(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor dataMart = infra.dataMart();
        return new MonitoredDataMartRemover(KnownPlugins.get(DataMartRemoverProvider.class, dataMart.pluginName(), dataMart.path() + ".type", "data mart remover")
                .create(dataMart.toPluginConfiguration()), monitorOf(infra, dataMart));
    }

    /** Lists the partitions of the layer of the data lake that a mount of the mode reads. */
    public static PartitionDiscovery partitionDiscovery(InfrastructureDescriptor infra, MountMode mode) {
        InfrastructureComponentDescriptor dataLake = infra.dataLake();
        return new MonitoredPartitionDiscovery(KnownPlugins.get(PartitionDiscoveryProvider.class, dataLake.pluginName(), dataLake.path() + ".type", "partition discovery")
                .create(PluginConfiguration.of(dataLake.configuration(), Map.of(PartitionDiscoveryProvider.MODE, mode))), monitorOf(infra, dataLake));
    }

    /** The metadata of the aggregates, when the infrastructure of the module has a piece for it. */
    public static Optional<AggregateMetadataRepository> aggregateMetadataRepository(InfrastructureDescriptor infra) {
        return infra.aggregateMetadata().map(piece -> new MonitoredAggregateMetadataRepository(KnownPlugins.get(AggregateMetadataRepositoryProvider.class,
                        piece.pluginName(), piece.path() + ".type", "aggregate metadata repository")
                .create(piece.toPluginConfiguration()), monitorOf(infra, piece)));
    }

    public static QuarantineRepository quarantineRepository(InfrastructureDescriptor infra) {
        InfrastructureComponentDescriptor quarantineRepository = infra.quarantineRepository();
        return new MonitoredQuarantineRepository(KnownPlugins.get(QuarantineRepositoryProvider.class, quarantineRepository.pluginName(), quarantineRepository.path() + ".type", "quarantine repository")
                .create(quarantineRepository.toPluginConfiguration()), monitorOf(infra, quarantineRepository));
    }

    public static CrawlerPorts crawlerPorts(InfrastructureDescriptor infra) {
        return CrawlerPorts.builder()
                .bus(tracedEventPublisher(infra))
                .inbox(inboxWriter(infra))
                .build();
    }

    private static PieceMonitor monitorOf(InfrastructureDescriptor infra, InfrastructureComponentDescriptor piece) {
        return infra.monitorOf(piece, PieceMonitors::of);
    }

    /** The source of a poll crawler, watched as a piece of the module with its own circuit. */
    public static PollSource<?> pollSource(InfrastructureDescriptor infra, SourceDescriptor source) {
        return monitored(KnownPlugins.get(PollSourceProvider.class, source.type(), "source.type", "poll source")
                .create(PluginConfiguration.of(source.configuration())), infra, source);
    }

    private static <R> PollSource<R> monitored(PollSource<R> created, InfrastructureDescriptor infra, SourceDescriptor source) {
        return new MonitoredPollSource<>(created,
                infra.monitorOf(created.sourceId(), () -> PieceMonitors.ofSource(created.sourceId(), source)));
    }

    public static Normalizer<?> normalizer(SourceDescriptor source) {
        return NormalizerComposer.withDeclaredSteps().compose(source.normalizer(), PluginConfiguration.builder()
                .with("rules", new String(RecordSerializerFactory.record().serialize(source.enrichment())))
                .with("rootPath", source.rootPath())
                .build());
    }
}
