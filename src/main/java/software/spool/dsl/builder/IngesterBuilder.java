package software.spool.dsl.builder;

import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.decorator.TraceEventPublisher;
import software.spool.core.port.inbox.InboxReader;
import software.spool.dsl.descriptors.infrastructure.*;
import software.spool.dsl.descriptors.module.ingester.FlushIngesterDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.*;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxUpdaterProvider;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.api.builder.IngesterBuilderFactory;
import software.spool.ingester.api.port.DataLakeWriter;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;
import java.util.Map;
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
                .from(eventBus)
                .flushPolicy(buildFlushPolicyFrom(ingester.flush()))
                .storesWith(getDataLakePluginFrom(infrastructure))
                .readWith(getInboxReaderPluginFrom(infrastructure))
                .quarantineStore(System.out::println)
                .on(TraceEventPublisher.of(eventBus).with(new OpenTelemetryTracedEventBus()))
                .create();
    }

    private static InboxReader getInboxReaderPluginFrom(InfrastructureDescriptor infrastructure) {
        return infrastructure.inbox().type() != InboxType.CUSTOM ?
                PluginResolver.get(InboxReaderProvider.class, infrastructure.inbox().type().name().toUpperCase())
                        .create(buildInboxConfigurationFrom(infrastructure.inbox())) :
                PluginResolver.get(InboxReaderProvider.class, infrastructure.inbox().custom().pluginName())
                        .create(buildConfigurationFrom(infrastructure.inbox().custom().configuration()));
    }

    private static DataLakeWriter getDataLakePluginFrom(InfrastructureDescriptor infrastructure) {
        return infrastructure.dataLake().type() != DataLakeType.CUSTOM ?
                PluginResolver.get(DataLakeWriterProvider.class, infrastructure.dataLake().type().name().toUpperCase())
                        .create(buildDataLakeConfigurationFrom(infrastructure.dataLake())) :
                PluginResolver.get(DataLakeWriterProvider.class, infrastructure.dataLake().custom().pluginName())
                        .create(buildConfigurationFrom(infrastructure.dataLake().custom().configuration()));
    }

    private static PluginConfiguration buildConfigurationFrom(Map<String, String> configuration) {
        PluginConfiguration.Builder builder = PluginConfiguration.builder();
        configuration.forEach(builder::with);
        return builder.build();
    }

    private static Ingester buildReactiveIngesterFrom(IngesterDescriptor ingester, InfrastructureDescriptor infrastructure, IngesterBuilderFactory.Configuration configuration) {
        EventBus eventBus = buildEventBusFrom(infrastructure);
        return configuration.reactive()
                .readWith(PluginResolver.resolve(InboxUpdaterProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .from(eventBus)
                .storesWith(getDataLakePluginFrom(infrastructure))
                .readWith(getInboxReaderPluginFrom(infrastructure))
                .quarantineStore(System.out::println)
                .on(TraceEventPublisher.of(eventBus).with(new OpenTelemetryTracedEventBus()))
                .create();
    }

    private static EventBus buildEventBusFrom(InfrastructureDescriptor infrastructure) {
        return PluginResolver.get(EventBusProvider.class, infrastructure.eventBus().type().name().toUpperCase()).create(buildBusConfigurationFrom(infrastructure.eventBus()));
    }

    private static FlushPolicy buildFlushPolicyFrom(FlushIngesterDescriptor flush) {
        return FlushPolicy.whenReaches(flush.size()).orEvery(Duration.ofMillis(flush.everyMilliseconds()));
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
