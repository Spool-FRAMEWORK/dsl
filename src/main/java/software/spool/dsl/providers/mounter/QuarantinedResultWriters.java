package software.spool.dsl.providers.mounter;

import software.spool.core.port.quarantine.QuarantinedResultWriter;
import software.spool.dsl.descriptors.DeclaredModule;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.mounter.MounterDescriptor;

import java.util.List;
import java.util.function.Supplier;

/**
 * The writers that put the quarantined results of the mounters of a node back in their data marts, one per mounter.
 * They are built only when a module asks for them, so a node without a janitor opens no data mart for nothing.
 */
public final class QuarantinedResultWriters {
    private final List<Supplier<QuarantinedResultWriter>> writers;

    private QuarantinedResultWriters(List<Supplier<QuarantinedResultWriter>> writers) {
        this.writers = List.copyOf(writers);
    }

    public static QuarantinedResultWriters of(SpoolNodeDescriptor node) {
        return new QuarantinedResultWriters(node.declaredModules().stream()
                .filter(module -> module.descriptor() instanceof MounterDescriptor)
                .map(module -> writerOf(node, module))
                .toList());
    }

    public static QuarantinedResultWriters none() {
        return new QuarantinedResultWriters(List.of());
    }

    public List<QuarantinedResultWriter> create() {
        return writers.stream().map(Supplier::get).toList();
    }

    private static Supplier<QuarantinedResultWriter> writerOf(SpoolNodeDescriptor node, DeclaredModule module) {
        return () -> MounterSpoolModuleProvider.quarantinedResultWriter((MounterDescriptor) module.descriptor(),
                node.infrastructureOf(module));
    }
}
