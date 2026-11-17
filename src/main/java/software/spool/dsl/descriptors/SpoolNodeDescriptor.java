package software.spool.dsl.descriptors;

import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import software.spool.dsl.descriptors.authentication.AuthenticationDescriptor;

import java.util.List;
import java.util.Map;

public record SpoolNodeDescriptor(
        InfrastructureDescriptor infrastructure,
        List<SpoolModuleDescriptor> modules,
        Map<String, AuthenticationDescriptor> authentication
) {
    public SpoolNodeDescriptor {
        authentication = authentication == null ? Map.of() : Map.copyOf(authentication);
    }

    public SpoolNodeDescriptor(InfrastructureDescriptor infrastructure, List<SpoolModuleDescriptor> modules) {
        this(infrastructure, modules, Map.of());
    }
}
