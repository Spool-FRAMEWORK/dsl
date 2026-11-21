package software.spool.dsl.descriptors;

import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.util.Map;

/**
 * A module as the descriptor declares it: where it is, what it is and the pieces it names in {@code uses}, already
 * checked against the infrastructure.
 */
public record DeclaredModule(
        String path,
        SpoolModuleDescriptor descriptor,
        Map<InfrastructureKind, InfrastructureComponentDescriptor> named
) {
    public DeclaredModule {
        named = Map.copyOf(named);
    }

    /** A module that names no piece, for code that builds a descriptor by hand. */
    public static DeclaredModule of(SpoolModuleDescriptor descriptor) {
        return new DeclaredModule("the module", descriptor, Map.of());
    }
}
