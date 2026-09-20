package software.spool.dsl.providers.ingester;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class IngesterModuleDescriptorProvider implements ModuleDescriptorProvider {

    @Override
    public String name() {
        return "INGESTER";
    }

    @Override
    public SpoolModuleDescriptor read(DescriptorReader ingester) {
        String id = ingester.string("id");
        DescriptorReader configuration = ingester.objectOrEmpty("configuration");
        configuration.optional("size", ValueParsers.INTEGER);
        configuration.optional("milliseconds", ValueParsers.LONG);
        return new IngesterDescriptor(
                ingester.optional("type", ValueParsers.STRING).orElse(null),
                id,
                ingester.stringMap("configuration")
        );
    }
}
