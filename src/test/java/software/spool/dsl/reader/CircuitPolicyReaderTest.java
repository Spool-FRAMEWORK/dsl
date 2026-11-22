package software.spool.dsl.reader;

import org.junit.jupiter.api.Test;
import software.spool.core.resilience.model.CircuitBreakerPolicy;
import software.spool.dsl.InvalidDescriptorException;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CircuitPolicyReaderTest {

    @Test
    void read_withoutKeys_givesTheDefaultPolicy() {
        CircuitBreakerPolicy policy = CircuitPolicyReader.read(configuration(Map.of()));

        assertThat(policy).isEqualTo(CircuitBreakerPolicy.defaults());
    }

    @Test
    void read_takesEachKeyThatIsDeclared() {
        CircuitBreakerPolicy policy = CircuitPolicyReader.read(configuration(Map.of(
                "circuitFailureRate", "100%", "circuitMinimumCalls", "2", "circuitWindow", "2m",
                "circuitOpenFor", "3s", "circuitHalfOpenCalls", 2)));

        assertThat(policy.failureRateThreshold()).isEqualTo(1.0f);
        assertThat(policy.minimumCalls()).isEqualTo(2);
        assertThat(policy.samplingWindow()).isEqualTo(Duration.ofMinutes(2));
        assertThat(policy.cooldown()).isEqualTo(Duration.ofSeconds(3));
        assertThat(policy.halfOpenPermits()).isEqualTo(2);
    }

    @Test
    void read_takesARateWithoutThePercentSign() {
        assertThat(CircuitPolicyReader.read(configuration(Map.of("circuitFailureRate", 25))).failureRateThreshold())
                .isEqualTo(0.25f);
    }

    @Test
    void read_rejectsARateOutsideItsRangeNamingTheKey() {
        assertThatThrownBy(() -> CircuitPolicyReader.read(configuration(Map.of("circuitFailureRate", "150%"))))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("source.configuration.circuitFailureRate")
                .hasMessageContaining("150%");
    }

    @Test
    void read_rejectsNoCallsAndNoTime() {
        assertThatThrownBy(() -> CircuitPolicyReader.read(configuration(Map.of("circuitMinimumCalls", "0"))))
                .hasMessageContaining("circuitMinimumCalls");
        assertThatThrownBy(() -> CircuitPolicyReader.read(configuration(Map.of("circuitOpenFor", "0s"))))
                .hasMessageContaining("circuitOpenFor");
        assertThatThrownBy(() -> CircuitPolicyReader.read(configuration(Map.of("circuitHalfOpenCalls", "0"))))
                .hasMessageContaining("circuitHalfOpenCalls");
    }

    private static DescriptorReader configuration(Map<String, ?> values) {
        return DescriptorReader.of("source.configuration", values);
    }
}
