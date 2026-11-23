package software.spool.dsl;

import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.providers.authentication.RequestAuthenticators;
import software.spool.dsl.providers.mounter.QuarantinedResultWriters;
import software.spool.dsl.registry.ModuleProviderRegistry;

import java.util.List;

/**
 * The modules a descriptor declares, built with their pieces, their authentication and their monitors, ready to be
 * registered in a node or added to a running one.
 */
public final class DescriptorModules {
    private DescriptorModules() {
    }

    public static List<SpoolModule> of(SpoolNodeDescriptor descriptor) {
        RequestAuthenticators authentication = RequestAuthenticators.from(descriptor.authentication());
        QuarantinedResultWriters resultWriters = QuarantinedResultWriters.of(descriptor);
        return descriptor.declaredModules().stream()
                .map(module -> ModuleProviderRegistry.build(module.descriptor(), descriptor.infrastructureOf(module),
                        authentication, resultWriters))
                .toList();
    }
}
