package software.spool.dsl.yaml.raw;

public record RawInfrastructureDescriptor(
        String watchdog,
        RawComponentDescriptor eventBus,
        RawComponentDescriptor inbox,
        RawComponentDescriptor dataLake
) {}