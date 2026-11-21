package software.spool.dsl.providers;

import software.spool.core.adapter.logging.LoggerFactory;
import software.spool.core.port.logging.Logger;
import software.spool.crawler.api.normalizer.NamedStep;
import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.crawler.api.normalizer.NormalizerCompositionException;
import software.spool.crawler.api.normalizer.NormalizerPlan;
import software.spool.crawler.api.normalizer.NormalizerStepCatalog;
import software.spool.crawler.api.normalizer.Stage;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Addition;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Extended;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.FromSteps;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Reference;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Replacement;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.StepReplacement;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.StretchReplacement;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.serde.ExtendableNormalizerProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Builds the normalizer a descriptor describes: a registered one as it is, extended at its stages, or made of the
 * steps declared with {@code @NormalizerStep}. Each combination that does not fit fails with the path where it is
 * written, and the effective normalizer is logged, so what runs can be read without the code.
 */
final class NormalizerComposer {
    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizerComposer.class);
    private static final String SUFFIX = "_NORMALIZER";

    private final Supplier<NormalizerStepCatalog> loadCatalog;
    private Optional<NormalizerStepCatalog> catalog = Optional.empty();

    private NormalizerComposer(Supplier<NormalizerStepCatalog> loadCatalog) {
        this.loadCatalog = loadCatalog;
    }

    /** A composer that looks the steps up in every catalog the class loader of the current thread can see. */
    static NormalizerComposer withDeclaredSteps() {
        return new NormalizerComposer(() -> NormalizerStepCatalog.load(Thread.currentThread().getContextClassLoader()));
    }

    Normalizer<?> compose(NormalizerDescriptor descriptor, PluginConfiguration configuration) {
        return switch (descriptor) {
            case Extended extended -> extended(extended, configuration);
            case FromSteps fromSteps -> fromSteps(fromSteps);
        };
    }

    private Normalizer<?> extended(Extended descriptor, PluginConfiguration configuration) {
        Reference from = descriptor.from();
        NormalizerProvider provider = KnownPlugins.get(NormalizerProvider.class, from.name(), SUFFIX, from.path(), "normalizer");
        if (!(provider instanceof ExtendableNormalizerProvider extendable)) {
            if (descriptor.changesNothing()) return provider.create(configuration);
            throw new InvalidDescriptorException(descriptor.path(), "cannot add or replace steps: the normalizer "
                    + from.name() + " publishes no stages. Name it alone, or build a normalizer from steps");
        }
        return extend(extendable.plan(configuration), descriptor);
    }

    private <I> Normalizer<I> extend(NormalizerPlan<I, Stream<byte[]>> plan, Extended descriptor) {
        for (Replacement replacement : descriptor.replace()) plan = replace(plan, replacement);
        for (Addition addition : descriptor.after()) {
            requireStage(plan, addition.stage(), addition.path());
            for (Reference step : addition.steps()) {
                NamedStep named = step(step);
                NormalizerPlan<I, Stream<byte[]>> current = plan;
                plan = composed(step.path(), () -> current.after(addition.stage(), List.of(named)));
            }
        }
        return built(plan, descriptor.path(), descriptor.path());
    }

    private <I> NormalizerPlan<I, Stream<byte[]>> replace(NormalizerPlan<I, Stream<byte[]>> plan, Replacement replacement) {
        return switch (replacement) {
            case StepReplacement single -> {
                requireStage(plan, single.stage().name(), single.stage().path());
                NamedStep with = step(single.with());
                yield composed(single.with().path(), () -> plan.replace(single.stage().name(), with));
            }
            case StretchReplacement stretch -> {
                requireStage(plan, stretch.from().name(), stretch.from().path());
                requireStage(plan, stretch.to().name(), stretch.to().path());
                List<NamedStep> with = stretch.with().stream().map(this::step).toList();
                yield composed(stretch.path() + ".with", () -> plan.replace(stretch.from().name(), stretch.to().name(), with));
            }
        };
    }

    private Normalizer<?> fromSteps(FromSteps descriptor) {
        NormalizerPlan<?, ?> plan = NormalizerPlan.from(Stage.RAW);
        for (Reference step : descriptor.steps()) {
            NamedStep named = step(step);
            NormalizerPlan<?, ?> before = plan;
            plan = composed(step.path(), () -> before.then(named));
        }
        return built(plan, descriptor.path() + ".steps", descriptor.path());
    }

    private NamedStep step(Reference reference) {
        NormalizerStepCatalog steps = composed(reference.path(), this::catalog);
        return composed(reference.path(), () -> steps.instantiate(reference.name())).orElseThrow(() ->
                new InvalidDescriptorException(reference.path(), "'" + reference.name()
                        + "' is not a known normalizer step. Known: " + String.join(", ", steps.names())));
    }

    private NormalizerStepCatalog catalog() {
        if (catalog.isEmpty()) catalog = Optional.of(loadCatalog.get());
        return catalog.get();
    }

    private static void requireStage(NormalizerPlan<?, ?> plan, String stage, String path) {
        if (plan.stages().contains(stage)) return;
        throw new InvalidDescriptorException(path, "'" + stage + "' is not a stage of the normalizer. Its stages are: "
                + String.join(", ", plan.stages()));
    }

    private static <I> Normalizer<I> built(NormalizerPlan<I, ?> plan, String errorPath, String descriptorPath) {
        Normalizer<I> normalizer = composed(errorPath, plan::build);
        LOGGER.info("The normalizer at {} runs {}", descriptorPath, plan.describe());
        return normalizer;
    }

    private static <T> T composed(String path, Supplier<T> composition) {
        try {
            return composition.get();
        } catch (NormalizerCompositionException e) {
            throw new InvalidDescriptorException(path, e.getMessage());
        }
    }
}
