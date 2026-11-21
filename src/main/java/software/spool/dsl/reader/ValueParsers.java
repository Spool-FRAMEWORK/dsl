package software.spool.dsl.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** The parsers most descriptors need. Anyone can write another by implementing {@link ValueParser}. */
public final class ValueParsers {

    /** Any plain YAML value read as text, so that {@code id: 5} is "5". Objects and lists are rejected. */
    public static final ValueParser<String> STRING = of("a text", raw ->
            raw instanceof String || raw instanceof Number || raw instanceof Boolean
                    ? Optional.of(String.valueOf(raw))
                    : Optional.empty());

    public static final ValueParser<Integer> INTEGER = of("an integer", raw ->
            whole(raw).filter(v -> v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE).map(Long::intValue));

    public static final ValueParser<Long> LONG = of("an integer", ValueParsers::whole);

    public static final ValueParser<Boolean> BOOLEAN = of("true or false", raw -> {
        if (raw instanceof Boolean value) return Optional.of(value);
        if (raw instanceof String text && (text.strip().equals("true") || text.strip().equals("false"))) {
            return Optional.of(Boolean.parseBoolean(text.strip()));
        }
        return Optional.empty();
    });

    /** A list whose entries are plain texts or objects with a {@code value}; the YAML accepts both. */
    public static final ValueParser<List<String>> TEXTS = of("a list of texts", raw -> {
        if (!(raw instanceof List<?> entries)) return Optional.empty();
        List<String> texts = new ArrayList<>();
        for (Object entry : entries) {
            Object value = entry instanceof Map<?, ?> object ? object.get("value") : entry;
            if (value == null) continue;
            Optional<String> text = STRING.parse(value);
            if (text.isEmpty()) return Optional.empty();
            texts.add(text.get());
        }
        return Optional.of(texts);
    });

    public static final ValueParser<Duration> DURATION = of("a duration such as 30d or P30D", ReadableDuration::parse);

    private ValueParsers() {}

    public static <T> ValueParser<T> of(String expected, Function<Object, Optional<T>> parse) {
        return new ValueParser<>() {
            @Override
            public Optional<T> parse(Object raw) {
                return parse.apply(raw);
            }

            @Override
            public String expected() {
                return expected;
            }
        };
    }

    /** One of the constants of an enum, by its exact name. The message lists the valid ones. */
    public static <E extends Enum<E>> ValueParser<E> oneOf(Class<E> type) {
        E[] constants = type.getEnumConstants();
        String allowed = Arrays.stream(constants).map(Enum::name).collect(Collectors.joining(", "));
        return of("one of " + allowed, raw -> raw instanceof String name
                ? Arrays.stream(constants).filter(c -> c.name().equals(name)).findFirst()
                : Optional.empty());
    }

    private static Optional<Long> whole(Object raw) {
        if (raw instanceof Integer || raw instanceof Long || raw instanceof Short || raw instanceof Byte) {
            return Optional.of(((Number) raw).longValue());
        }
        if (raw instanceof String text && text.strip().matches("-?\\d+")) {
            try {
                return Optional.of(Long.parseLong(text.strip()));
            } catch (NumberFormatException tooBig) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }
}
