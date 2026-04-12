package software.spool.dsl.builder;

import software.spool.core.port.bus.EventBus;
import software.spool.dsl.EventBusFactory;
import software.spool.dsl.InboxReaderFactory;
import software.spool.dsl.InboxUpdaterFactory;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.feeder.FeederDescriptor;
import software.spool.feeder.api.Feeder;
import software.spool.feeder.api.builder.FeederBuilderFactory;

import java.time.Duration;
import java.util.Objects;

public class FeederBuilder {
    public static Feeder buildFrom(FeederDescriptor feeder, InfrastructureDescriptor infrastructure) {
        FeederBuilderFactory.Configuration configuration = FeederBuilderFactory.watchdog(
                infrastructure.watchdog().url(), feeder.id());
        return Objects.isNull(feeder.poll()) ?
                buildReactiveFeederFrom(feeder, infrastructure, configuration) :
                buildPollingFeederFrom(feeder, infrastructure, configuration);
    }

    private static Feeder buildPollingFeederFrom(FeederDescriptor feeder, InfrastructureDescriptor infrastructure, FeederBuilderFactory.Configuration configuration) {
        return configuration.polling()
                .every(Duration.ofSeconds(feeder.poll().every()))
                .from(InboxReaderFactory.from(infrastructure.inbox()))
                .on(EventBusFactory.from(infrastructure.eventBus()))
                .with(InboxUpdaterFactory.from(infrastructure.inbox()))
                .create();
    }

    private static Feeder buildReactiveFeederFrom(FeederDescriptor feeder, InfrastructureDescriptor infrastructure, FeederBuilderFactory.Configuration configuration) {
        EventBus eventBus = EventBusFactory.from(infrastructure.eventBus());
        return configuration.reactive()
                .from(eventBus)
                .on(eventBus)
                .with(InboxUpdaterFactory.from(infrastructure.inbox()))
                .create();

    }
}
