package software.spool.dsl.providers.ingester;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterType;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;

import static software.spool.dsl.reader.ReadableDuration.insteadOfMilliseconds;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class IngesterModuleDescriptorProvider implements ModuleDescriptorProvider {

    @Override
    public String name() {
        return "INGESTER";
    }

    @Override
    public SpoolModuleDescriptor read(DescriptorReader ingester) {
        DescriptorReader configuration = ingester.objectOrEmpty("configuration")
                .replaced("milliseconds", insteadOfMilliseconds("every"));
        return new IngesterDescriptor(
                ingester.required("type", ValueParsers.oneOf(IngesterType.class)),
                ingester.string("id"),
                configuration.optional("size", ValueParsers.INTEGER),
                configuration.optional("every", ValueParsers.DURATION),
                configuration.optional("processedKeysWindow", ValueParsers.DURATION)
        );
    }
}
