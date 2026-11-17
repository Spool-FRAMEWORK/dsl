package software.spool.dsl.providers.janitor;

import software.spool.dsl.descriptors.module.ScheduleDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;

import static software.spool.dsl.reader.ReadableDuration.insteadOfMilliseconds;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class JanitorModuleDescriptorProvider implements ModuleDescriptorProvider {

    @Override
    public String name() {
        return "JANITOR";
    }

    @Override
    public SpoolModuleDescriptor read(DescriptorReader janitor) {
        DescriptorReader configuration = janitor.objectOrEmpty("configuration")
                .replaced("milliseconds", insteadOfMilliseconds("schedule.every"))
                .replaced("millisecondsThreshold", insteadOfMilliseconds("threshold"))
                .replaced("millisecondsTTL", insteadOfMilliseconds("ttl"))
                .replaced("millisecondsQuarantineTTL", insteadOfMilliseconds("quarantineTtl"))
                .replaced("millisecondsQuarantineRetention", insteadOfMilliseconds("quarantineRetention"));
        return new JanitorDescriptor(
                janitor.string("id"),
                ScheduleDescriptor.of(janitor),
                configuration.optional("threshold", ValueParsers.DURATION),
                configuration.optional("ttl", ValueParsers.DURATION),
                configuration.optional("quarantineTtl", ValueParsers.DURATION),
                configuration.optional("quarantineRetention", ValueParsers.DURATION),
                configuration.optional("maxInboxUpdateAttempts", ValueParsers.INTEGER)
        );
    }
}
