package software.spool.dsl.providers.janitor;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class JanitorModuleDescriptorProvider implements ModuleDescriptorProvider {

    @Override
    public String name() {
        return "JANITOR";
    }

    @Override
    public SpoolModuleDescriptor read(DescriptorReader janitor) {
        String id = janitor.string("id");
        DescriptorReader configuration = janitor.objectOrEmpty("configuration");
        configuration.optional("milliseconds", ValueParsers.LONG);
        configuration.optional("millisecondsThreshold", ValueParsers.INTEGER);
        configuration.optional("millisecondsTTL", ValueParsers.INTEGER);
        configuration.optional("millisecondsQuarantineTTL", ValueParsers.INTEGER);
        return new JanitorDescriptor(id, janitor.stringMap("configuration"));
    }
}
