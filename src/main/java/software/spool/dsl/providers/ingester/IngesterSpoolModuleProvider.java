package software.spool.dsl.providers.ingester;

import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterType;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.ingester.api.builder.IngesterBuilderFactory;
import software.spool.ingester.internal.utils.FlushPolicy;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Set;

@SpoolPlugin(SpoolModuleProvider.class)
public class IngesterSpoolModuleProvider implements SpoolModuleProvider {
    private static final int DEFAULT_BATCH_SIZE = 100;

    @Override
    public String name() {
        return "INGESTER";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public Set<InfrastructureKind> uses(SpoolModuleDescriptor descriptor) {
        return Set.of(InfrastructureKind.EVENT_BUS, InfrastructureKind.INBOX, InfrastructureKind.DATA_LAKE,
                InfrastructureKind.ENVELOPE_HEADERS);
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
        ingester.processedKeysWindow().ifPresent(builder::rememberProcessedKeysFor);
        return builder
                .from(bus)
                .storesWith(InfrastructurePluginFactory.dataLakeWriter(infrastructure))
                .readWith(InfrastructurePluginFactory.inboxReader(infrastructure))
                .on(InfrastructurePluginFactory.tracedEventPublisher(infrastructure))
                .create();
    }

    private boolean isBuffered(IngesterDescriptor ingester) {
        return ingester.type() == IngesterType.FLUSH;
    }

    private FlushPolicy buildFlushPolicy(IngesterDescriptor ingester) {
        FlushPolicy policy = FlushPolicy.whenReaches(ingester.size().orElse(DEFAULT_BATCH_SIZE));
        return ingester.every().map(policy::orEvery).orElse(policy);
    }
}