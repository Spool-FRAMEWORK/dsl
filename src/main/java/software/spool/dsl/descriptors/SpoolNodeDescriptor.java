package software.spool.dsl.descriptors;

import software.spool.dsl.descriptors.authentication.AuthenticationDescriptor;
import software.spool.dsl.descriptors.infrastructure.DeclaredInfrastructure;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public record SpoolNodeDescriptor(
        Optional<String> watchdog,
        DeclaredInfrastructure infrastructure,
        List<DeclaredModule> declaredModules,
        Map<String, AuthenticationDescriptor> authentication
) {
    public SpoolNodeDescriptor {
        declaredModules = List.copyOf(declaredModules);
        authentication = authentication == null ? Map.of() : Map.copyOf(authentication);
    }

    public SpoolNodeDescriptor(Optional<String> watchdog, DeclaredInfrastructure infrastructure, List<DeclaredModule> modules) {
        this(watchdog, infrastructure, modules, Map.of());
    }

    public List<SpoolModuleDescriptor> modules() {
        return declaredModules.stream().map(DeclaredModule::descriptor).toList();
    }

    /** A new view of the infrastructure for the module, each time, since it remembers what the module asks for. */
    public InfrastructureDescriptor infrastructureOf(DeclaredModule module) {
        return new InfrastructureDescriptor(watchdog, infrastructure, module.path(), module.named());
    }
}
