package software.spool.dsl.yaml.raw;

import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;

public record RawInfrastructureDescriptor(
        String watchdog,
        RawComponentDescriptor eventBus,
        RawComponentDescriptor inbox,
        RawComponentDescriptor dataLake,
        RawComponentDescriptor quarantineRepository,
        RawComponentDescriptor quarantineStore,
        RawComponentDescriptor dataMart
) {
    public RawInfrastructureDescriptor(String watchdog, RawComponentDescriptor eventBus,
                                       RawComponentDescriptor inbox, RawComponentDescriptor dataLake) {
        this(watchdog, eventBus, inbox, dataLake, null, null, null);
    }

    static RawInfrastructureDescriptor from(DescriptorReader infrastructure) {
        infrastructure.knownKeysOnly(RawInfrastructureDescriptor.class);
        return new RawInfrastructureDescriptor(
                infrastructure.optional("watchdog", ValueParsers.STRING).orElse(null),
                component(infrastructure, "eventBus"),
                component(infrastructure, "inbox"),
                component(infrastructure, "dataLake"),
                component(infrastructure, "quarantineRepository"),
                component(infrastructure, "quarantineStore"),
                component(infrastructure, "dataMart")
        );
    }

    private static RawComponentDescriptor component(DescriptorReader infrastructure, String key) {
        return infrastructure.value(key).map(value -> RawComponentDescriptor.from(infrastructure.object(key)))
                .orElse(null);
    }
}
