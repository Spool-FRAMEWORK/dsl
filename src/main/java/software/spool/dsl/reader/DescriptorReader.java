package software.spool.dsl.reader;

import software.spool.dsl.InvalidDescriptorException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A typed view of one object in a descriptor that knows where it is. Every error it raises names the
 * path of the key, what was expected and what was found, so nobody writes those messages by hand.
 *
 * <pre>{@code
 * DescriptorReader source = crawler.object("source");               // modules[0].crawler.source
 * Integer every = source.optional("every", ValueParsers.INTEGER)    // "... must be an integer, got 'abc'"
 *                       .orElse(null);
 * }</pre>
 */
public final class DescriptorReader {
    private final String path;
    private final Map<?, ?> values;

    private DescriptorReader(String path, Map<?, ?> values) {
        this.path = path;
        this.values = values;
    }

    /** An object read from a map. A null map, like the empty body of {@code - crawler:}, reads as empty. */
    public static DescriptorReader of(String path, Map<?, ?> values) {
        return new DescriptorReader(path, values == null ? Map.of() : values);
    }

    /** An object read from a value found inside another one: it must be an object or nothing at all. */
    public static DescriptorReader ofValue(String path, Object value) {
        if (value == null) return of(path, null);
        if (value instanceof Map<?, ?> map) return of(path, map);
        throw new InvalidDescriptorException(path, "must be an object, got '" + value + "'");
    }

    /** Fails with "is required" when a value that has to be there is not. */
    public static <T> T require(String path, T value) {
        if (value == null) throw new InvalidDescriptorException(path, "is required");
        return value;
    }

    public String path() {
        return path;
    }

    public Set<String> keys() {
        Set<String> keys = new LinkedHashSet<>();
        values.keySet().forEach(key -> keys.add(String.valueOf(key)));
        return keys;
    }

    public <T> T required(String key, ValueParser<T> parser) {
        return parse(key, require(child(key), values.get(key)), parser);
    }

    public <T> Optional<T> optional(String key, ValueParser<T> parser) {
        Object raw = values.get(key);
        return raw == null ? Optional.empty() : Optional.of(parse(key, raw, parser));
    }

    public String string(String key) {
        return required(key, ValueParsers.STRING);
    }

    /** A nested object that has to be there. */
    public DescriptorReader object(String key) {
        return ofValue(child(key), require(child(key), values.get(key)));
    }

    /** A nested object that may be missing, in which case it reads as empty. */
    public DescriptorReader objectOrEmpty(String key) {
        return ofValue(child(key), values.get(key));
    }

    /** A list of objects that may be missing, in which case it is empty. */
    public List<DescriptorReader> list(String key) {
        Object value = values.get(key);
        if (value == null) return List.of();
        if (!(value instanceof List<?> items)) {
            throw new InvalidDescriptorException(child(key), "must be a list, got '" + value + "'");
        }
        List<DescriptorReader> readers = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            readers.add(ofValue(child(key) + "[" + i + "]", items.get(i)));
        }
        return readers;
    }

    /** An object of free keys with plain values, kept as text. Empty if missing. */
    public Map<String, String> stringMap(String key) {
        Object value = values.get(key);
        if (value == null) return Map.of();
        if (!(value instanceof Map<?, ?> map)) {
            throw new InvalidDescriptorException(child(key), "must be an object, got '" + value + "'");
        }
        Map<String, String> result = new LinkedHashMap<>();
        map.forEach((k, v) -> result.put(String.valueOf(k), String.valueOf(v)));
        return result;
    }

    private <T> T parse(String key, Object raw, ValueParser<T> parser) {
        return parser.parse(raw).orElseThrow(() -> new InvalidDescriptorException(
                child(key), "must be " + parser.expected() + ", got '" + raw + "'"));
    }

    private String child(String key) {
        return path.isEmpty() ? key : path + "." + key;
    }
}
