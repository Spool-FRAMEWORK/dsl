package software.spool.dsl.builder;

import software.spool.core.adapter.otel.OpenTelemetryTracedEventBus;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.decorator.TraceEventPublisher;
import software.spool.core.port.inbox.InboxEnvelopeRemover;
import software.spool.core.port.inbox.InboxReader;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.dsl.descriptors.infrastructure.EventBusDescriptor;
import software.spool.dsl.descriptors.infrastructure.InboxDescriptor;
import software.spool.dsl.descriptors.infrastructure.InboxType;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxEnvelopeRemoverProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxReaderProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxUpdaterProvider;
import software.spool.janitor.api.Janitor;
import software.spool.janitor.api.builder.JanitorBuilderFactory;
import software.spool.infrastructure.spi.provider.*;

import java.time.Duration;
import java.util.Map;

public class JanitorBuilder {
    public static Janitor buildFrom(JanitorDescriptor janitor, InfrastructureDescriptor infrastructure) {
        JanitorBuilderFactory.Configuration configuration = JanitorBuilderFactory.watchdog(
                infrastructure.watchdog().url(), janitor.id());
        return buildPollingFeederFrom(janitor, infrastructure, configuration);
    }

    private static EventBus buildEventBusFrom(InfrastructureDescriptor infrastructure) {
        return PluginResolver.get(EventBusProvider.class, infrastructure.eventBus().type().name().toUpperCase()).create(buildBusConfigurationFrom(infrastructure.eventBus()));
    }

    private static Janitor buildPollingFeederFrom(JanitorDescriptor janitor, InfrastructureDescriptor infrastructure, JanitorBuilderFactory.Configuration configuration) {
        return configuration.polling()
                .every(Duration.ofMillis(janitor.everyMilliseconds()))
                .from(getInboxReaderFrom(infrastructure))
                .on(TraceEventPublisher.of(buildEventBusFrom(infrastructure)).with(new OpenTelemetryTracedEventBus()))
                .with(getInboxUpdaterFrom(infrastructure))
                .removeWith(getInboxRemoverFrom(infrastructure))
                .subscribeWith(buildEventBusFrom(infrastructure))
                .withMillisecondsThreshold(janitor.millisecondsThreshold())
                .withMillisecondsTtl(janitor.millisecondsTtl())
                .create();
    }

    private static InboxEnvelopeRemover getInboxRemoverFrom(InfrastructureDescriptor infrastructure) {
        return infrastructure.inbox().type() != InboxType.CUSTOM ? PluginResolver.get(InboxEnvelopeRemoverProvider.class,
                        infrastructure.inbox().type().name().toUpperCase())
                .create(buildInboxConfigurationFrom(infrastructure.inbox())) :
                PluginResolver.get(InboxEnvelopeRemoverProvider.class,
                                infrastructure.inbox().custom().pluginName())
                        .create(buildConfigurationFrom(infrastructure.inbox().custom().configuration()));
    }

    private static InboxReader getInboxReaderFrom(InfrastructureDescriptor infrastructure) {
        return infrastructure.inbox().type() != InboxType.CUSTOM ? PluginResolver.get(InboxReaderProvider.class,
                        infrastructure.inbox().type().name().toUpperCase())
                .create(buildInboxConfigurationFrom(infrastructure.inbox())) :
                PluginResolver.get(InboxReaderProvider.class,
                                infrastructure.inbox().custom().pluginName())
                        .create(buildConfigurationFrom(infrastructure.inbox().custom().configuration()));
    }

    private static InboxUpdater getInboxUpdaterFrom(InfrastructureDescriptor infrastructure) {
        return infrastructure.inbox().type() != InboxType.CUSTOM ? PluginResolver.get(InboxUpdaterProvider.class,
                        infrastructure.inbox().type().name().toUpperCase())
                .create(buildInboxConfigurationFrom(infrastructure.inbox())) :
                PluginResolver.get(InboxUpdaterProvider.class,
                                infrastructure.inbox().custom().pluginName())
                        .create(buildConfigurationFrom(infrastructure.inbox().custom().configuration()));
    }

    private static PluginConfiguration buildConfigurationFrom(Map<String, String> configuration) {
        PluginConfiguration.Builder builder = PluginConfiguration.builder();
        configuration.forEach(builder::with);
        return builder.build();
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
