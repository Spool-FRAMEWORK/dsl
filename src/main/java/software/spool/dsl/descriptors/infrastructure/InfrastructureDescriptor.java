package software.spool.dsl.descriptors.infrastructure;

public record InfrastructureDescriptor(
        String watchdog,
        InfrastructureComponentDescriptor eventBus,
        InfrastructureComponentDescriptor inbox,
        InfrastructureComponentDescriptor dataLake,
        InfrastructureComponentDescriptor quarantineRepository
) {
    public InfrastructureDescriptor(String watchdog, InfrastructureComponentDescriptor eventBus,
                                    InfrastructureComponentDescriptor inbox, InfrastructureComponentDescriptor dataLake) {
        this(watchdog, eventBus, inbox, dataLake, null);
    }
}
