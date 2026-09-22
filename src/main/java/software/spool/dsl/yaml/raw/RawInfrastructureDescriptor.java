package software.spool.dsl.yaml.raw;

public record RawInfrastructureDescriptor(
        String watchdog,
        RawComponentDescriptor eventBus,
        RawComponentDescriptor inbox,
        RawComponentDescriptor dataLake,
        RawComponentDescriptor quarantineStore
) {
    public RawInfrastructureDescriptor(String watchdog, RawComponentDescriptor eventBus,
                                       RawComponentDescriptor inbox, RawComponentDescriptor dataLake) {
        this(watchdog, eventBus, inbox, dataLake, null);
    }
}
