package software.spool.dsl.descriptors.module.crawler.source;

import java.util.List;

/**
 * The {@code normalizer} of a source: a normalizer named and maybe extended at its stages, or one built from steps.
 * Every part keeps its path in the descriptor, so a combination that does not fit is reported where it is written.
 */
public sealed interface NormalizerDescriptor {

    String path();

    /** A name written in the descriptor, with the path where it is written. */
    record Reference(String name, String path) {}

    /** A registered normalizer, with the steps it replaces and then the ones added after its stages. */
    record Extended(String path, Reference from, List<Replacement> replace, List<Addition> after)
            implements NormalizerDescriptor {

        /** A registered normalizer used as it is, named at the given path. */
        public static Extended named(String name, String path) {
            return new Extended(path, new Reference(name, path), List.of(), List.of());
        }

        public boolean changesNothing() {
            return replace.isEmpty() && after.isEmpty();
        }
    }

    /** A normalizer made only of named steps, from the raw payload to the records. */
    record FromSteps(String path, List<Reference> steps) implements NormalizerDescriptor {}

    /** Steps that run after a stage, in the order written. */
    record Addition(String stage, String path, List<Reference> steps) {}

    sealed interface Replacement {
        String path();
    }

    /** The step that gives a stage, replaced by another one. */
    record StepReplacement(String path, Reference stage, Reference with) implements Replacement {}

    /** Every step from one stage to a later one, replaced by a chain of steps. */
    record StretchReplacement(String path, Reference from, Reference to, List<Reference> with) implements Replacement {}
}
