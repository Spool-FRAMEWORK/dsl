package software.spool.dsl.registry;

import software.spool.core.adapter.health.MonitoredModule;
import software.spool.dsl.providers.mounter.QuarantinedResultWriters;
import software.spool.dsl.providers.authentication.RequestAuthenticators;
import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

public final class ModuleProviderRegistry {

    public static final String AUTHENTICATION = "authentication";
    public static final String QUARANTINED_RESULT_WRITERS = "quarantinedResultWriters";

    private ModuleProviderRegistry() {}

    public static SpoolModule build(SpoolModuleDescriptor descriptor,
                                    InfrastructureDescriptor infrastructure) {
        return build(descriptor, infrastructure, RequestAuthenticators.none());
    }

    /**
     * Builds a module, giving its provider the authentication providers of the descriptor in the context key
     * {@value #AUTHENTICATION}, so that an HTTP entry of the module can find the one it names. Once built, a piece the
     * module names in {@code uses} but never asked for fails, since it is almost certainly a mistake. The module built
     * reports the health of each piece it asked for.
     */
    public static SpoolModule build(SpoolModuleDescriptor descriptor,
                                    InfrastructureDescriptor infrastructure,
                                    RequestAuthenticators authentication) {
        return build(descriptor, infrastructure, authentication, QuarantinedResultWriters.none());
    }

    /**
     * Builds a module as {@link #build(SpoolModuleDescriptor, InfrastructureDescriptor, RequestAuthenticators)} does,
     * also giving its provider, in the context key {@value #QUARANTINED_RESULT_WRITERS}, the writers that put the
     * quarantined results of the mounters of its node back in their data marts.
     */
    public static SpoolModule build(SpoolModuleDescriptor descriptor,
                                    InfrastructureDescriptor infrastructure,
                                    RequestAuthenticators authentication,
                                    QuarantinedResultWriters resultWriters) {
        PluginConfiguration config = PluginConfiguration.builder()
                .with("descriptor", descriptor)
                .with("infrastructure", infrastructure)
                .with(AUTHENTICATION, authentication)
                .with(QUARANTINED_RESULT_WRITERS, resultWriters)
                .build();

        try {
            SpoolModule module = PluginResolver.resolve(SpoolModuleProvider.class, config);
            infrastructure.checkEveryNamedPieceIsUsed();
            return new MonitoredModule(module, infrastructure.monitors());
        } catch (InvalidDescriptorException e) {
            throw e.inModule(descriptor.id());
        } catch (IllegalStateException e) {
            if (supportedBySomeProvider(config)) throw e;
            throw new InvalidDescriptorException("module '" + descriptor.id() + "'",
                    "of type " + descriptor.moduleType() + " is not supported. Supported types: " + supportedTypes());
        }
    }

    /** The kinds of piece the module of the descriptor uses, as its provider declares them. */
    public static Set<InfrastructureKind> uses(SpoolModuleDescriptor descriptor) {
        PluginConfiguration config = PluginConfiguration.builder().with("descriptor", descriptor).build();
        return PluginResolver.available(SpoolModuleProvider.class).values().stream()
                .filter(provider -> provider.supports(config))
                .min(Comparator.comparingInt(SpoolModuleProvider::priority))
                .map(provider -> provider.uses(descriptor))
                .orElseThrow(() -> new InvalidDescriptorException("module '" + descriptor.id() + "'",
                        "of type " + descriptor.moduleType() + " is not supported. Supported types: " + supportedTypes()));
    }

    /** If some provider does support the module, the failure is something else and must not be hidden. */
    private static boolean supportedBySomeProvider(PluginConfiguration config) {
        return PluginRegistry.findAll(SpoolModuleProvider.class).values().stream()
                .anyMatch(provider -> provider.supports(config));
    }

    private static String supportedTypes() {
        return PluginRegistry.findAll(SpoolModuleProvider.class).keySet().stream()
                .sorted()
                .collect(Collectors.joining(", "));
    }
}
