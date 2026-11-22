package software.spool.dsl.providers;

import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.infrastructure.spi.Plugin;

import java.util.Set;

public interface SpoolModuleProvider extends Plugin<SpoolModule> {
    /**
     * The kinds of piece the module of the descriptor uses, also the ones it can go without, so that what is generated
     * for it (its role in AWS) reaches them and nothing else. Building the module asks for exactly these.
     */
    Set<InfrastructureKind> uses(SpoolModuleDescriptor descriptor);
}
