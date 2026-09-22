package software.spool.dsl.descriptors.infrastructure;

public record InfrastructureDescriptor(
        String watchdog,
        InfrastructureComponentDescriptor eventBus,
        InfrastructureComponentDescriptor inbox,
        InfrastructureComponentDescriptor dataLake,
        InfrastructureComponentDescriptor quarantineStore
) {
    public InfrastructureDescriptor(String watchdog, InfrastructureComponentDescriptor eventBus,
                                    InfrastructureComponentDescriptor inbox, InfrastructureComponentDescriptor dataLake) {
        this(watchdog, eventBus, inbox, dataLake, null);
    }
}
