package software.spool.dsl.yaml;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.resolver.Resolver;
import software.spool.core.exception.DeserializationException;
import software.spool.core.port.serde.PayloadDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads a descriptor file into a tree of maps, lists and single values, with YAML anchors and aliases expanded.
 *
 * <p>
 * Jackson keeps an alias such as {@code *local} as the text {@code local}, so the descriptor is read with
 * SnakeYAML instead. Dates are kept as text, as Jackson did, so every other value keeps the type it always had.
 * Only plain YAML types are built: no tag can create an arbitrary Java object.
 * </p>
 */
public final class YamlTreeParser implements PayloadDeserializer<Map<String, Object>> {
    private static final String DOCUMENT = "the YAML descriptor";

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> deserialize(byte[] payload) throws DeserializationException {
        Object document;
        try {
            document = yaml().load(new String(payload, StandardCharsets.UTF_8));
        } catch (YAMLException e) {
            throw new DeserializationException(DOCUMENT, e.getMessage());
        }
        if (document == null) return new LinkedHashMap<>();
        if (!(document instanceof Map<?, ?> tree)) {
            throw new DeserializationException(DOCUMENT, "A descriptor must be an object, got '" + document + "'");
        }
        return new LinkedHashMap<>((Map<String, Object>) tree);
    }

    private static Yaml yaml() {
        LoaderOptions options = new LoaderOptions();
        return new Yaml(new SafeConstructor(options), new Representer(new DumperOptions()), new DumperOptions(), options,
                new ResolverWithoutTimestamps());
    }

    /** SnakeYAML's resolver without the implicit timestamp type, which would turn {@code 2026-01-01} into a date. */
    private static final class ResolverWithoutTimestamps extends Resolver {
        @Override
        protected void addImplicitResolvers() {
            addImplicitResolver(Tag.BOOL, BOOL, "yYnNtTfFoO", 10);
            addImplicitResolver(Tag.INT, INT, "-+0123456789");
            addImplicitResolver(Tag.FLOAT, FLOAT, "-+0123456789.");
            addImplicitResolver(Tag.MERGE, MERGE, "<", 10);
            addImplicitResolver(Tag.NULL, NULL, "~nN\0", 10);
            addImplicitResolver(Tag.NULL, EMPTY, null, 10);
            addImplicitResolver(Tag.YAML, YAML, "!&*", 10);
        }
    }
}
