package software.spool.dsl.registry;

import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

public final class ModuleProviderRegistry {

    private ModuleProviderRegistry() {}

    public static SpoolModule build(SpoolModuleDescriptor descriptor,
                                    InfrastructureDescriptor infrastructure) {
        PluginConfiguration config = PluginConfiguration.builder()
                .with("descriptor", descriptor)
                .with("infrastructure", infrastructure)
                .build();

        return PluginResolver.resolve(SpoolModuleProvider.class, config);
    }
}