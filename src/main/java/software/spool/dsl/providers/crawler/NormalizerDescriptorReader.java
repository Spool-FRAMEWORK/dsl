package software.spool.dsl.providers.crawler;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Addition;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Extended;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.FromSteps;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Reference;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Replacement;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.StepReplacement;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.StretchReplacement;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads the {@code normalizer} of a source: a name ({@code normalizer: JSON_ARRAY}), a named normalizer extended at its
 * stages ({@code {from, replace, after}}) or one built from steps ({@code {steps}}).
 */
final class NormalizerDescriptorReader {
    private static final String KEY = "normalizer";
    private static final String STEPS = "steps";
    private static final List<String> EXTENSION_KEYS = List.of("from", "replace", "after");

    private NormalizerDescriptorReader() {}

    static NormalizerDescriptor read(DescriptorReader source) {
        String path = child(source.path(), KEY);
        Object raw = source.value(KEY).orElseThrow(() -> missing(source, path));
        if (!(raw instanceof Map<?, ?>)) {
            return Extended.named(source.string(KEY), path);
        }
        DescriptorReader normalizer = source.object(KEY);
        return normalizer.keys().contains(STEPS) ? fromSteps(normalizer) : extended(normalizer);
    }

    private static InvalidDescriptorException missing(DescriptorReader source, String path) {
        String example = source.optional("mediaType", ValueParsers.STRING).orElse("JSON_ARRAY");
        return new InvalidDescriptorException(path, "is required. The media type no longer chooses the normalizer:"
                + " write 'normalizer: " + example + "' and, in mediaType, the media type of the records the source"
                + " writes, such as 'mediaType: JSON'");
    }

    private static FromSteps fromSteps(DescriptorReader normalizer) {
        EXTENSION_KEYS.stream().filter(normalizer.keys()::contains).findFirst().ifPresent(key -> {
            throw new InvalidDescriptorException(child(normalizer.path(), STEPS), "cannot be combined with "
                    + child(normalizer.path(), key) + ": a normalizer is either built from steps or extends another one");
        });
        return new FromSteps(normalizer.path(), references(normalizer, STEPS));
    }

    private static Extended extended(DescriptorReader normalizer) {
        String from = normalizer.string("from");
        return new Extended(
                normalizer.path(),
                new Reference(from, child(normalizer.path(), "from")),
                normalizer.list("replace").stream().map(NormalizerDescriptorReader::replacement).toList(),
                additions(normalizer.objectOrEmpty("after")));
    }

    private static List<Addition> additions(DescriptorReader after) {
        return after.keys().stream()
                .map(stage -> new Addition(stage, child(after.path(), stage), references(after, stage)))
                .toList();
    }

    private static Replacement replacement(DescriptorReader replacement) {
        if (replacement.keys().contains("between")) {
            List<Reference> between = references(replacement, "between");
            if (between.size() != 2) {
                throw new InvalidDescriptorException(child(replacement.path(), "between"),
                        "must name two stages, the first and the last of the stretch, got " + between.size());
            }
            return new StretchReplacement(replacement.path(), between.get(0), between.get(1),
                    references(replacement, "with"));
        }
        return new StepReplacement(replacement.path(),
                new Reference(replacement.string("stage"), child(replacement.path(), "stage")),
                new Reference(replacement.string("with"), child(replacement.path(), "with")));
    }

    private static List<Reference> references(DescriptorReader parent, String key) {
        String path = child(parent.path(), key);
        Object raw = parent.value(key).orElseThrow(() -> new InvalidDescriptorException(path, "is required"));
        if (!(raw instanceof List<?> names)) {
            throw new InvalidDescriptorException(path, "must be a list of names, got '" + raw + "'");
        }
        List<Reference> references = new ArrayList<>();
        for (int index = 0; index < names.size(); index++) {
            String elementPath = path + "[" + index + "]";
            Object name = names.get(index);
            String text = ValueParsers.STRING.parse(name).orElseThrow(() ->
                    new InvalidDescriptorException(elementPath, "must be a name, got '" + name + "'"));
            references.add(new Reference(text, elementPath));
        }
        return references;
    }

    private static String child(String path, String key) {
        return path.isEmpty() ? key : path + "." + key;
    }
}
