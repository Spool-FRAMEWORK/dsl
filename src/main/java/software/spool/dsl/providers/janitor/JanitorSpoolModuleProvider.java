package software.spool.dsl.providers.janitor;

import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.janitor.api.builder.JanitorBuilderFactory;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.time.Duration;

@SpoolPlugin(SpoolModuleProvider.class)
public class JanitorSpoolModuleProvider implements SpoolModuleProvider {

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
        return JanitorBuilderFactory.watchdog(infrastructure.watchdog(), janitor.id())
                .polling()
                .from(InfrastructurePluginFactory.inboxReader(infrastructure))
                .with(InfrastructurePluginFactory.inboxUpdater(infrastructure))
                .removeWith(InfrastructurePluginFactory.inboxEnvelopeRemover(infrastructure))
                .on(InfrastructurePluginFactory.tracedEventPublisher(infrastructure))
                .subscribeWith(bus)
                .every(buildInterval(janitor))
                .withMillisecondsThreshold(buildWorkers(janitor))
                .withMillisecondsTtl(buildTtl(janitor))
                .create();
    }

    private Duration buildInterval(JanitorDescriptor janitor) {
        String millis = janitor.configuration().get("milliseconds");
        return Duration.ofMillis(millis != null ? Long.parseLong(millis) : 5000L);
    }

    private Integer buildWorkers(JanitorDescriptor janitor) {
        String threshold = janitor.configuration().get("millisecondsThreshold");
        return threshold != null ? Integer.parseInt(threshold) : null;
    }

    private Integer buildTtl(JanitorDescriptor janitor) {
        String ttl = janitor.configuration().get("millisecondsTTL");
        return ttl != null ? Integer.parseInt(ttl) : null;
    }
}