package software.spool.dsl.compose;

import software.spool.core.port.serde.PayloadDeserializer;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.source.DescriptorSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Combines a descriptor with the files it includes and applies the profile chosen for it, as ADR-016
 * decides. Maps are combined key by key and any other value, lists included, is replaced whole.
 * Included files come first, in the order of the list, and the file that includes them goes on top.
 */
public final class DescriptorComposer {
    public static final String INCLUDE = "include";
    public static final String PROFILES = "profiles";

    private final DescriptorSource source;
    private final PayloadDeserializer<Map<String, Object>> parser;

    public DescriptorComposer(DescriptorSource source, PayloadDeserializer<Map<String, Object>> parser) {
        this.source = source;
        this.parser = parser;
    }

    public ComposedDescriptor compose(String root, Optional<String> profile) throws IOException {
        Map<String, Object> rootTree = read(root).orElseThrow(
                () -> new FileNotFoundException("Descriptor not found in " + source.name() + ": " + root));
        DescriptorOrigins origins = new DescriptorOrigins(Origin.of(root));
        Map<String, Object> tree = new LinkedHashMap<>();
        combine(root, rootTree, new ArrayList<>(), tree, origins);
        Map<String, Object> profiles = profilesOf(tree.remove(PROFILES), origins, root);
        if (profile.isPresent()) apply(profile.get(), profiles, tree, origins, root);
        return new ComposedDescriptor(tree, origins);
    }

    private void combine(String location, Map<String, Object> file, List<String> chain,
                         Map<String, Object> tree, DescriptorOrigins origins) throws IOException {
        chain.add(location);
        String origin = Origin.of(location).describe(chain.get(0));
        List<String> names = namesOf(file.remove(INCLUDE), origin);
        for (int i = 0; i < names.size(); i++) {
            String target = source.resolve(location, names.get(i));
            if (chain.contains(target)) {
                List<String> cycle = new ArrayList<>(chain.subList(chain.indexOf(target), chain.size()));
                cycle.add(target);
                throw new InvalidDescriptorException(INCLUDE, "forms a cycle: " + String.join(" -> ", cycle))
                        .from(origin);
            }
            int index = i;
            Map<String, Object> included = read(target).orElseThrow(() -> new InvalidDescriptorException(
                    INCLUDE + "[" + index + "]", "names " + target + ", which is not in " + source.name())
                    .from(origin));
            combine(target, included, chain, tree, origins);
        }
        merge(tree, file, "", path -> Origin.of(location), origins);
        chain.remove(chain.size() - 1);
    }

    private static List<String> namesOf(Object includes, String origin) {
        if (includes == null) return List.of();
        if (!(includes instanceof List<?> items)) {
            throw new InvalidDescriptorException(INCLUDE, "must be a list of files, got '" + includes + "'")
                    .from(origin);
        }
        List<String> names = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            if (!(items.get(i) instanceof String name)) {
                throw new InvalidDescriptorException(INCLUDE + "[" + i + "]",
                        "must be the path of a file, got '" + items.get(i) + "'").from(origin);
            }
            names.add(name);
        }
        return names;
    }

    private static Map<String, Object> profilesOf(Object value, DescriptorOrigins origins, String root) {
        if (value == null) return Map.of();
        if (!(value instanceof Map<?, ?> map)) {
            throw new InvalidDescriptorException(PROFILES, "must be an object, got '" + value + "'")
                    .from(origins.originOf(PROFILES).describe(root));
        }
        Map<String, Object> profiles = new LinkedHashMap<>();
        map.forEach((name, body) -> profiles.put(String.valueOf(name), body));
        return profiles;
    }

    private static void apply(String name, Map<String, Object> profiles, Map<String, Object> tree,
                              DescriptorOrigins origins, String root) {
        if (!profiles.containsKey(name)) {
            throw new InvalidDescriptorException(PROFILES, "does not declare '" + name + "'. Known profiles: "
                    + (profiles.isEmpty() ? "none" : String.join(", ", profiles.keySet())));
        }
        String profilePath = PROFILES + "." + name;
        Object body = profiles.get(name);
        if (body != null && !(body instanceof Map<?, ?>)) {
            throw new InvalidDescriptorException(profilePath, "must be an object, got '" + body + "'")
                    .from(origins.originOf(profilePath).describe(root));
        }
        Map<?, ?> overrides = body == null ? Map.of() : (Map<?, ?>) body;
        for (String forbidden : List.of(INCLUDE, PROFILES)) {
            if (overrides.containsKey(forbidden)) {
                String path = profilePath + "." + forbidden;
                throw new InvalidDescriptorException(path, "is not allowed inside a profile")
                        .from(origins.originOf(path).describe(root));
            }
        }
        merge(tree, overrides, "", path -> origins.originOf(profilePath + "." + path).inProfile(name), origins);
    }

    @SuppressWarnings("unchecked")
    private static void merge(Map<String, Object> target, Map<?, ?> overlay, String path,
                              Function<String, Origin> originOf, DescriptorOrigins origins) {
        overlay.forEach((rawKey, value) -> {
            String key = String.valueOf(rawKey);
            String childPath = path.isEmpty() ? key : path + "." + key;
            Origin origin = originOf.apply(childPath);
            Object current = target.get(key);
            if (value instanceof Map<?, ?> nested && current instanceof Map<?, ?>) {
                origins.record(childPath, origin);
                merge((Map<String, Object>) current, nested, childPath, originOf, origins);
                return;
            }
            target.put(key, copyOf(value));
            origins.record(childPath, origin);
        });
    }

    private static Object copyOf(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, nested) -> copy.put(String.valueOf(key), copyOf(nested)));
            return copy;
        }
        if (value instanceof List<?> list) return new ArrayList<>(list.stream().map(DescriptorComposer::copyOf).toList());
        return value;
    }

    private Optional<Map<String, Object>> read(String location) throws IOException {
        Optional<byte[]> content = source.read(location);
        if (content.isEmpty()) return Optional.empty();
        Map<String, Object> tree = parser.deserialize(content.get());
        return Optional.of(tree == null ? new LinkedHashMap<>() : new LinkedHashMap<>(tree));
    }
}
