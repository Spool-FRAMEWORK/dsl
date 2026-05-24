package software.spool.dsl.descriptors.infrastructure;

public record InfrastructureDescriptor(
        String watchdog,
        InfrastructureComponentDescriptor eventBus,
        InfrastructureComponentDescriptor inbox,
        InfrastructureComponentDescriptor dataLake
) {
}
