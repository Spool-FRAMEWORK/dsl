package software.spool.dsl.providers;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

/**
 * Reads the descriptor of one kind of module. Its name is the key that introduces the module in the YAML,
 * so {@code name() = "CRAWLER"} handles every {@code - crawler:} entry. A new kind of module is a new
 * class annotated with {@code @SpoolPlugin(ModuleDescriptorProvider.class)}, nothing else has to change.
 */
public interface ModuleDescriptorProvider extends Plugin<SpoolModuleDescriptor> {

    /** Reads what is under the module key, for example everything under {@code - crawler:}. */
    SpoolModuleDescriptor read(DescriptorReader module);

    @Override
    default int priority() {
        return 10;
    }

    @Override
    default boolean supports(PluginConfiguration configuration) {
        return true;
    }

    @Override
    default SpoolModuleDescriptor create(PluginConfiguration configuration) {
        return read(configuration.require("reader", DescriptorReader.class));
    }
}
