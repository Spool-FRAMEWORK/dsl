package software.spool.dsl;

import software.spool.dsl.descriptors.DeclaredModule;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.DeclaredInfrastructure;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Pieces of infrastructure for tests, named after their kind as a descriptor would usually name them. */
public final class Pieces {
    private Pieces() {}

    public static InfrastructureComponentDescriptor eventBus(String type) {
        return new InfrastructureComponentDescriptor("eventBus", InfrastructureKind.EVENT_BUS, type, Map.of());
    }

    public static InfrastructureComponentDescriptor inbox(String type, Map<String, String> configuration) {
        return new InfrastructureComponentDescriptor("inbox", InfrastructureKind.INBOX, type, configuration);
    }

    public static InfrastructureComponentDescriptor dataLake(String type, Map<String, String> configuration) {
        return new InfrastructureComponentDescriptor("dataLake", InfrastructureKind.DATA_LAKE, type, configuration);
    }

    public static InfrastructureComponentDescriptor quarantineRepository(String type, Map<String, String> configuration) {
        return new InfrastructureComponentDescriptor("quarantineRepository", InfrastructureKind.QUARANTINE_REPOSITORY,
                type, configuration);
    }

    /** The infrastructure of a module that names no piece. */
    public static InfrastructureDescriptor infrastructure(InfrastructureComponentDescriptor... pieces) {
        return InfrastructureDescriptor.unnamed(Optional.empty(), DeclaredInfrastructure.of(pieces));
    }

    public static SpoolNodeDescriptor node(List<? extends SpoolModuleDescriptor> modules,
                                           InfrastructureComponentDescriptor... pieces) {
        List<DeclaredModule> declared = modules.stream().map(module -> DeclaredModule.of(module)).toList();
        return new SpoolNodeDescriptor(Optional.empty(), DeclaredInfrastructure.of(pieces), declared);
    }
}
