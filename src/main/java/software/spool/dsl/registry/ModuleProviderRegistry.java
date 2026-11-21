package software.spool.dsl.registry;

import software.spool.dsl.providers.authentication.RequestAuthenticators;
import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.stream.Collectors;

public final class ModuleProviderRegistry {

    public static final String AUTHENTICATION = "authentication";

    private ModuleProviderRegistry() {}

    public static SpoolModule build(SpoolModuleDescriptor descriptor,
                                    InfrastructureDescriptor infrastructure) {
        return build(descriptor, infrastructure, RequestAuthenticators.none());
    }

    /**
     * Builds a module, giving its provider the authentication providers of the descriptor in the context key
     * {@value #AUTHENTICATION}, so that an HTTP entry of the module can find the one it names. Once built, a piece the
     * module names in {@code uses} but never asked for fails, since it is almost certainly a mistake.
     */
    public static SpoolModule build(SpoolModuleDescriptor descriptor,
                                    InfrastructureDescriptor infrastructure,
                                    RequestAuthenticators authentication) {
        PluginConfiguration config = PluginConfiguration.builder()
                .with("descriptor", descriptor)
                .with("infrastructure", infrastructure)
                .with(AUTHENTICATION, authentication)
                .build();

        try {
            SpoolModule module = PluginResolver.resolve(SpoolModuleProvider.class, config);
            infrastructure.checkEveryNamedPieceIsUsed();
            return module;
        } catch (InvalidDescriptorException e) {
            throw e.inModule(descriptor.id());
        } catch (IllegalStateException e) {
            if (supportedBySomeProvider(config)) throw e;
            throw new InvalidDescriptorException("module '" + descriptor.id() + "'",
                    "of type " + descriptor.moduleType() + " is not supported. Supported types: " + supportedTypes());
        }
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
