package software.spool.dsl.builder;

import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.port.decorator.TraceEventPublisher;
import software.spool.dsl.descriptors.infrastructure.EventBusDescriptor;
import software.spool.dsl.descriptors.infrastructure.InboxDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.feeder.JanitorDescriptor;
import software.spool.feeder.api.Janitor;
import software.spool.feeder.api.builder.JanitorBuilderFactory;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.spi.provider.*;

import java.time.Duration;

public class JanitorBuilder {
    public static Janitor buildFrom(JanitorDescriptor feeder, InfrastructureDescriptor infrastructure) {
        JanitorBuilderFactory.Configuration configuration = JanitorBuilderFactory.watchdog(
                infrastructure.watchdog().url(), feeder.id());
        return buildPollingFeederFrom(feeder, infrastructure, configuration);
    }

    private static Janitor buildPollingFeederFrom(JanitorDescriptor feeder, InfrastructureDescriptor infrastructure, JanitorBuilderFactory.Configuration configuration) {
        return configuration.polling()
                .every(Duration.ofSeconds(feeder.poll().every()))
                .from(PluginRegistry.resolve(InboxReaderProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .on(TraceEventPublisher.of(PluginRegistry.resolve(EventBusProvider.class, buildBusConfigurationFrom(infrastructure.eventBus()))).with(new OpenTelemetryTracedEventBus()))
                .with(PluginRegistry.resolve(InboxUpdaterProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .removeWith(PluginRegistry.resolve(InboxEnvelopeRemoverProvider.class, buildInboxConfigurationFrom(infrastructure.inbox())))
                .subscribeWith(PluginRegistry.resolve(EventBusProvider.class, buildBusConfigurationFrom(infrastructure.eventBus())))
                .create();
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
