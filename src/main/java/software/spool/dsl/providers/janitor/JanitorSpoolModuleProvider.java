package software.spool.dsl.providers.janitor;

import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.ScheduleDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.janitor.api.builder.JanitorBuilderFactory;
import software.spool.janitor.api.builder.PollingJanitorBuilder;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.time.Duration;

@SpoolPlugin(SpoolModuleProvider.class)
public class JanitorSpoolModuleProvider implements SpoolModuleProvider {
    private static final Duration DEFAULT_ROUND_INTERVAL = Duration.ofSeconds(5);
    private static final Duration LONGEST_INT_MILLIS = Duration.ofMillis(Integer.MAX_VALUE);

    @Override
    public String name() {
        return "JANITOR";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.get("descriptor", SpoolModuleDescriptor.class)
                .filter(d -> d instanceof JanitorDescriptor)
                .isPresent();
    }

    @Override
    public SpoolModule create(PluginConfiguration configuration) {
        JanitorDescriptor janitor = configuration.require("descriptor", JanitorDescriptor.class);
        InfrastructureDescriptor infrastructure = configuration.require("infrastructure", InfrastructureDescriptor.class);
        var bus = InfrastructurePluginFactory.eventBus(infrastructure);
        PollingJanitorBuilder builder = JanitorBuilderFactory.watchdog(infrastructure.watchdog(), janitor.id())
                .polling()
                .from(InfrastructurePluginFactory.inboxReader(infrastructure))
                .with(InfrastructurePluginFactory.inboxUpdater(infrastructure))
                .removeWith(InfrastructurePluginFactory.inboxEnvelopeRemover(infrastructure))
                .on(InfrastructurePluginFactory.tracedEventPublisher(infrastructure))
                .subscribeWith(bus)
                .quarantineIn(InfrastructurePluginFactory.quarantineRepository(infrastructure))
                .every(janitor.schedule().map(ScheduleDescriptor::every).orElse(DEFAULT_ROUND_INTERVAL));
        janitor.threshold().ifPresent(threshold -> builder.withMillisecondsThreshold(intMillis("threshold", threshold)));
        janitor.ttl().ifPresent(ttl -> builder.withMillisecondsTtl(intMillis("ttl", ttl)));
        janitor.quarantineTtl().ifPresent(ttl -> builder.withMillisecondsQuarantineTtl(intMillis("quarantineTtl", ttl)));
        janitor.quarantineRetention().ifPresent(retention -> builder.withMillisecondsQuarantineRetention(retention.toMillis()));
        janitor.maxInboxUpdateAttempts().ifPresent(builder::withMaxInboxUpdateAttempts);
        if (infrastructure.dataLake() != null) {
            builder.rewriteBronzeIn(InfrastructurePluginFactory.bronzeRepository(infrastructure));
        }
        return builder.create();
    }

    /** The janitor takes some times as an int of milliseconds; a longer one fails instead of wrapping around. */
    private static int intMillis(String key, Duration duration) {
        if (duration.compareTo(LONGEST_INT_MILLIS) > 0) {
            throw new InvalidDescriptorException("configuration." + key,
                    "must be at most " + LONGEST_INT_MILLIS + ", got " + duration);
        }
        return (int) duration.toMillis();
    }
}
