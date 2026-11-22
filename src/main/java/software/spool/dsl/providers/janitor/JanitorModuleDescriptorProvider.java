package software.spool.dsl.providers.janitor;

import software.spool.dsl.descriptors.module.ScheduleReader;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.descriptors.module.janitor.QuarantineServerDescriptor;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;

import static software.spool.dsl.reader.ReadableDuration.insteadOfMilliseconds;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class JanitorModuleDescriptorProvider implements ModuleDescriptorProvider {
    private static final ScheduleReader SCHEDULE = ScheduleReader.standard();

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
                SCHEDULE.read(janitor),
                configuration.optional("threshold", ValueParsers.DURATION),
                configuration.optional("ttl", ValueParsers.DURATION),
                configuration.optional("quarantineTtl", ValueParsers.DURATION),
                configuration.optional("quarantineRetention", ValueParsers.DURATION),
                configuration.optional("maxInboxUpdateAttempts", ValueParsers.INTEGER),
                janitor.value("quarantineServer").map(present -> quarantineServer(janitor.object("quarantineServer")))
        );
    }

    private static QuarantineServerDescriptor quarantineServer(DescriptorReader server) {
        DescriptorReader roles = server.object("roles");
        return new QuarantineServerDescriptor(
                server.string("authentication"),
                roles.string("read"),
                roles.string("resolve"),
                roles.string("bypass"),
                server.optional("pageSize", ValueParsers.INTEGER),
                server.optional("maxPageSize", ValueParsers.INTEGER));
    }
}
