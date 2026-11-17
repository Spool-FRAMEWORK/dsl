package software.spool.dsl.reader;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A duration as a person writes it in the descriptor: ISO 8601 ({@code PT5S}, {@code P30D}) or the short form
 * ({@code 100ms}, {@code 5s}, {@code 10m}, {@code 2h}, {@code 30d}), as ADR-011 decides. A bare number is not a
 * duration, because nothing would say its unit.
 */
public final class ReadableDuration {
    private static final Pattern SHORT_FORM = Pattern.compile("(\\d+)(ms|s|m|h|d)");
    private static final Map<String, ChronoUnit> UNITS_FROM_LARGEST = unitsFromLargest();

    private ReadableDuration() {}

    static Optional<Duration> parse(Object raw) {
        if (!(raw instanceof String text)) return Optional.empty();
        return shortForm(text.strip()).or(() -> iso(text.strip())).filter(duration -> !duration.isNegative());
    }

    /**
     * What to write instead of a key that held milliseconds, with its own value as the example when it is a number
     * written in the descriptor: {@code millisecondsTTL: 100000} becomes {@code use ttl with a duration such as 100s}.
     * A value from the environment arrives as text and is never echoed, not even converted.
     */
    public static Function<Object, String> insteadOfMilliseconds(String replacement) {
        return raw -> "use " + replacement + " with a duration such as "
                + (raw instanceof Number millis ? shortFormOf(Duration.ofMillis(millis.longValue())) : "30s");
    }

    private static String shortFormOf(Duration duration) {
        for (Map.Entry<String, ChronoUnit> unit : UNITS_FROM_LARGEST.entrySet()) {
            long unitMillis = unit.getValue().getDuration().toMillis();
            if (duration.toMillis() % unitMillis == 0) return duration.toMillis() / unitMillis + unit.getKey();
        }
        return duration.toMillis() + "ms";
    }

    private static Optional<Duration> shortForm(String text) {
        Matcher matcher = SHORT_FORM.matcher(text);
        if (!matcher.matches()) return Optional.empty();
        try {
            return Optional.of(Duration.of(Long.parseLong(matcher.group(1)), UNITS_FROM_LARGEST.get(matcher.group(2))));
        } catch (NumberFormatException | ArithmeticException tooLarge) {
            return Optional.empty();
        }
    }

    private static Optional<Duration> iso(String text) {
        try {
            return Optional.of(Duration.parse(text));
        } catch (DateTimeParseException notIso) {
            return Optional.empty();
        }
    }

    private static Map<String, ChronoUnit> unitsFromLargest() {
        Map<String, ChronoUnit> units = new LinkedHashMap<>();
        units.put("d", ChronoUnit.DAYS);
        units.put("h", ChronoUnit.HOURS);
        units.put("m", ChronoUnit.MINUTES);
        units.put("s", ChronoUnit.SECONDS);
        units.put("ms", ChronoUnit.MILLIS);
        return units;
    }
}
