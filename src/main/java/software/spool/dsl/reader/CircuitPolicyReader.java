package software.spool.dsl.reader;

import software.spool.core.resilience.model.CircuitBreakerPolicy;

import java.time.Duration;
import java.util.Optional;

/**
 * Reads the circuit of a piece or a source from its configuration. Each key is optional and falls back to the
 * default of {@link CircuitBreakerPolicy}: {@code circuitFailureRate} (such as {@code 50%}), {@code circuitMinimumCalls},
 * {@code circuitWindow}, {@code circuitOpenFor} and {@code circuitHalfOpenCalls}.
 */
public final class CircuitPolicyReader {
    public static final String FAILURE_RATE = "circuitFailureRate";
    public static final String MINIMUM_CALLS = "circuitMinimumCalls";
    public static final String WINDOW = "circuitWindow";
    public static final String OPEN_FOR = "circuitOpenFor";
    public static final String HALF_OPEN_CALLS = "circuitHalfOpenCalls";

    private static final float WHOLE = 100f;

    private static final ValueParser<Float> PERCENTAGE = ValueParsers.of("a percentage above 0% and up to 100%, such as 50%",
            raw -> ValueParsers.STRING.parse(raw)
                    .map(text -> text.strip().endsWith("%") ? text.strip().substring(0, text.strip().length() - 1) : text.strip())
                    .flatMap(CircuitPolicyReader::number)
                    .filter(percent -> percent > 0 && percent <= WHOLE)
                    .map(percent -> percent / WHOLE));

    private static final ValueParser<Integer> AT_LEAST_ONE = ValueParsers.of("an integer of at least 1",
            raw -> ValueParsers.INTEGER.parse(raw).filter(value -> value >= 1));

    private static final ValueParser<Duration> SOME_TIME = ValueParsers.of("a duration longer than zero, such as 30s",
            raw -> ValueParsers.DURATION.parse(raw).filter(duration -> !duration.isZero()));

    private CircuitPolicyReader() {
    }

    public static CircuitBreakerPolicy read(DescriptorReader configuration) {
        return CircuitBreakerPolicy.timeBased()
                .withFailureRateThreshold(configuration.optional(FAILURE_RATE, PERCENTAGE)
                        .orElse(CircuitBreakerPolicy.DEFAULT_FAILURE_RATE))
                .withMinimumCalls(configuration.optional(MINIMUM_CALLS, AT_LEAST_ONE)
                        .orElse(CircuitBreakerPolicy.DEFAULT_MINIMUM_CALLS))
                .withSamplingWindow(configuration.optional(WINDOW, SOME_TIME).orElse(CircuitBreakerPolicy.DEFAULT_WINDOW))
                .withCooldown(configuration.optional(OPEN_FOR, SOME_TIME).orElse(CircuitBreakerPolicy.DEFAULT_OPEN_FOR))
                .withHalfOpenPermits(configuration.optional(HALF_OPEN_CALLS, AT_LEAST_ONE)
                        .orElse(CircuitBreakerPolicy.DEFAULT_HALF_OPEN_CALLS))
                .build();
    }

    private static Optional<Float> number(String text) {
        try {
            return Optional.of(Float.parseFloat(text));
        } catch (NumberFormatException notANumber) {
            return Optional.empty();
        }
    }
}
