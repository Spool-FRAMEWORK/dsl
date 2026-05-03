package software.spool.dsl.builder;

import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.decorator.TraceEventPublisher;
import software.spool.dsl.descriptors.infrastructure.DataLakeDescriptor;
import software.spool.dsl.descriptors.infrastructure.EventBusDescriptor;
import software.spool.dsl.descriptors.infrastructure.InboxDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.ingester.FlushIngesterDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.spi.provider.*;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.api.builder.IngesterBuilderFactory;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;
import java.util.Objects;

public class IngesterBuilder {
    public static Ingester buildFrom(IngesterDescriptor ingester, InfrastructureDescriptor infrastructure) {
        EventBus eventBus = buildEventBusFrom(infrastructure);
        IngesterBuilderFactory.Configuration configuration = IngesterBuilderFactory.watchdog(
                infrastructure.watchdog().url(), ingester.id());
        return Objects.isNull(ingester.flush()) ?
                buildReactiveIngesterFrom(ingester, infrastructure, configuration) :
                buildBufferedIngesterFrom(ingester, infrastructure, configuration);
    }

    private static Ingester buildBufferedIngesterFrom(IngesterDescriptor ingester, InfrastructureDescriptor infrastructure, IngesterBuilderFactory.Configuration configuration) {
        EventBus eventBus = buildEventBusFrom(infrastructure);
        return configuration.buffered()
                .readWith(PluginRegistry.resolve(InboxUpdaterProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .from(eventBus)
                .flushPolicy(buildFlushPolicyFrom(ingester.flush()))
                .storesWith(PluginRegistry.resolve(DataLakeWriterProvider.class, buildDataLakeConfigurationFrom(infrastructure.dataLake())))
                .readWith(PluginRegistry.resolve(InboxReaderProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .quarantineStore(System.out::println)
                .on(TraceEventPublisher.of(eventBus).with(new OpenTelemetryTracedEventBus()))
                .create();
    }

    private static Ingester buildReactiveIngesterFrom(IngesterDescriptor ingester, InfrastructureDescriptor infrastructure, IngesterBuilderFactory.Configuration configuration) {
        EventBus eventBus = buildEventBusFrom(infrastructure);
        return configuration.reactive()
                .readWith(PluginRegistry.resolve(InboxUpdaterProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .from(eventBus)
                .storesWith(PluginRegistry.resolve(DataLakeWriterProvider.class, buildDataLakeConfigurationFrom(infrastructure.dataLake())))
                .readWith(PluginRegistry.resolve(InboxReaderProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .quarantineStore(System.out::println)
                .on(TraceEventPublisher.of(eventBus).with(new OpenTelemetryTracedEventBus()))
                .create();
    }

    private static EventBus buildEventBusFrom(InfrastructureDescriptor infrastructure) {
        return PluginRegistry.resolve(EventBusProvider.class, buildBusConfigurationFrom(infrastructure.eventBus()));
    }

    private static FlushPolicy buildFlushPolicyFrom(FlushIngesterDescriptor flush) {
        return FlushPolicy.whenReaches(flush.size()).orEvery(Duration.ofSeconds(flush.every()));
    }

    private static PluginConfiguration buildDataLakeConfigurationFrom(DataLakeDescriptor inboxDescriptor) {
        return PluginConfiguration.builder()
                .with("endpoint", inboxDescriptor.s3().endpoint())
                .with("region", inboxDescriptor.s3().region())
                .with("bucket", inboxDescriptor.s3().bucket()).build();
    }

    private static PluginConfiguration buildBusConfigurationFrom(EventBusDescriptor eventBusDescriptor) {
        return PluginConfiguration.builder()
                .with("bootstrap.servers", eventBusDescriptor.url()).build();
    }

    private static PluginConfiguration buildInboxConfigurationFrom(InboxDescriptor inboxDescriptor) {
        return PluginConfiguration.builder()
                .with("endpoint", inboxDescriptor.s3().endpoint())
                .with("region", inboxDescriptor.s3().region())
                .with("bucket", inboxDescriptor.s3().bucket()).build();
    }
}
