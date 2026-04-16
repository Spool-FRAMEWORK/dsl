package software.spool.dsl.descriptors;

import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.util.List;

public record SpoolNodeDescriptor(
        InfrastructureDescriptor infrastructure,
        List<SpoolModuleDescriptor> modules
) {
}
