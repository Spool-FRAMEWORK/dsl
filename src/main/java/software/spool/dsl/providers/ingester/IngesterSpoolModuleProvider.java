package software.spool.dsl.providers.ingester;

import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.ingester.api.builder.IngesterBuilderFactory;
import software.spool.ingester.internal.utils.FlushPolicy;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.time.Duration;

@SpoolPlugin(SpoolModuleProvider.class)
public class IngesterSpoolModuleProvider implements SpoolModuleProvider {
    @Override
    public String name() {
        return "INGESTER";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.get("descriptor", SpoolModuleDescriptor.class)
                .filter(d -> d instanceof IngesterDescriptor)
                .isPresent();
    }

    @Override
    public SpoolModule create(PluginConfiguration configuration) {
        IngesterDescriptor ingester = configuration.require("descriptor", IngesterDescriptor.class);
        InfrastructureDescriptor infrastructure = configuration.require("infrastructure", InfrastructureDescriptor.class);
        var bus = InfrastructurePluginFactory.eventBus(infrastructure);
        var factory = infrastructure.watchdog() != null
                ? IngesterBuilderFactory.watchdog(infrastructure.watchdog(), ingester.id())
                : IngesterBuilderFactory.watchdog(null, ingester.id());
        var builder = isBuffered(ingester)
                ? factory.buffered().flushPolicy(buildFlushPolicy(ingester))
                : factory.reactive();
        return builder
                .from(bus)
                .storesWith(InfrastructurePluginFactory.dataLakeWriter(infrastructure))
                .readWith(InfrastructurePluginFactory.inboxReader(infrastructure))
                .on(InfrastructurePluginFactory.tracedEventPublisher(infrastructure))
                .create();
    }

    private boolean isBuffered(IngesterDescriptor ingester) {
        return "FLUSH".equalsIgnoreCase(ingester.type());
    }

    private FlushPolicy buildFlushPolicy(IngesterDescriptor ingester) {
        String size = ingester.configuration().get("size");
        String millis = ingester.configuration().get("milliseconds");
        FlushPolicy policy = size != null
                ? FlushPolicy.whenReaches(Integer.parseInt(size))
                : FlushPolicy.whenReaches(100);
        return millis != null
                ? policy.orEvery(Duration.ofMillis(Long.parseLong(millis)))
                : policy;
    }
}