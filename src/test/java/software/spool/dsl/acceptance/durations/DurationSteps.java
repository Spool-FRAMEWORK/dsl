package software.spool.dsl.acceptance.durations;

import io.cucumber.java.en.Then;
import software.spool.core.model.schedule.Schedule;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DurationSteps {
    private static final Map<String, ChronoUnit> UNITS = Map.of(
            "days", ChronoUnit.DAYS,
            "hours", ChronoUnit.HOURS,
            "minutes", ChronoUnit.MINUTES,
            "seconds", ChronoUnit.SECONDS,
            "milliseconds", ChronoUnit.MILLIS);

    private final DescriptorUnderTest descriptor;

    public DurationSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Then("the quarantine retention of the janitor is {int} {word}")
    public void theQuarantineRetentionIs(int amount, String unit) {
        assertThat(janitor().quarantineRetention()).contains(Duration.of(amount, UNITS.get(unit)));
    }

    @Then("the janitor runs a round every {int} minute(s)")
    public void theJanitorRunsARoundEvery(int minutes) {
        assertThat(janitor().schedule()).contains(Schedule.every(Duration.ofMinutes(minutes)));
    }

    @Then("the janitor republishes after {int} minutes, expires after {int} minutes and leaves the quarantine after {int} minutes")
    public void theJanitorTimes(int threshold, int ttl, int quarantineTtl) {
        JanitorDescriptor janitor = janitor();
        assertThat(janitor.threshold()).contains(Duration.ofMinutes(threshold));
        assertThat(janitor.ttl()).contains(Duration.ofMinutes(ttl));
        assertThat(janitor.quarantineTtl()).contains(Duration.ofMinutes(quarantineTtl));
    }

    @Then("the ingester flushes every {int} minutes or every {int} envelopes")
    public void theIngesterFlushes(int minutes, int size) {
        IngesterDescriptor ingester = module(IngesterDescriptor.class);
        assertThat(ingester.every()).contains(Duration.ofMinutes(minutes));
        assertThat(ingester.size()).contains(size);
    }

    @Then("the crawler polls every {int} minutes")
    public void theCrawlerPollsEvery(int minutes) {
        assertThat(module(CrawlerDescriptor.class).schedule()).isEqualTo(Schedule.every(Duration.ofMinutes(minutes)));
    }

    private JanitorDescriptor janitor() {
        return module(JanitorDescriptor.class);
    }

    private <T> T module(Class<T> type) {
        return descriptor.loaded().modules().stream().filter(type::isInstance).map(type::cast).findFirst()
                .orElseThrow(() -> new AssertionError("the descriptor has no " + type.getSimpleName()));
    }
}
