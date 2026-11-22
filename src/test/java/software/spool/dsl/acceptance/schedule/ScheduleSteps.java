package software.spool.dsl.acceptance.schedule;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import software.spool.core.model.schedule.Schedule;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.descriptors.module.mounter.MounterDescriptor;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduleSteps {
    private static final String INFRASTRUCTURE = """
            infrastructure:
              eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
              inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/schedule/inbox}}
              dataLake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: target/spool-test/schedule/lake}}
              quarantineRepository: {kind: QUARANTINE_REPOSITORY, type: FILE_SYSTEM, configuration: {path: target/spool-test/schedule/quarantine}}
            modules:
            """;
    private static final String CRAWLER = """
              - crawler:
                  type: POLL
                  id: crawler
            %s      source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: schedule, url: "http://localhost/event"}}
                  eventMapping: {namingConvention: SNAKE_CASE}
            """;
    private static final String JANITOR = """
              - janitor:
                  id: janitor
            %s""";
    private static final String MOUNTER = """
              - mounter:
                  id: mounter
                  type: POLL
            %s      mode: TRANSFORMATION
                  read: {partitions: ["source=payments"]}
                  write: {dataMart: payments, partitioning: {type: SAME_AS_SOURCE}}
            """;
    private static final Map<String, Function<DescriptorUnderTest, Optional<Schedule>>> SCHEDULE_OF_MODULE = Map.of(
            "crawler", descriptor -> Optional.of(module(descriptor, CrawlerDescriptor.class).schedule()),
            "janitor", descriptor -> module(descriptor, JanitorDescriptor.class).schedule(),
            "mounter", descriptor -> module(descriptor, MounterDescriptor.class).schedule());

    private final DescriptorUnderTest descriptor;

    public ScheduleSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("a crawler whose schedule is {}")
    public void aCrawlerWhoseScheduleIs(String schedule) {
        descriptor.write(INFRASTRUCTURE + CRAWLER.formatted(scheduleLine(schedule)));
    }

    @Given("a crawler without schedule")
    public void aCrawlerWithoutSchedule() {
        descriptor.write(INFRASTRUCTURE + CRAWLER.formatted(""));
    }

    @Given("a janitor whose schedule is {}")
    public void aJanitorWhoseScheduleIs(String schedule) {
        descriptor.write(INFRASTRUCTURE + JANITOR.formatted(scheduleLine(schedule)));
    }

    @Given("a polling mounter whose schedule is {}")
    public void aPollingMounterWhoseScheduleIs(String schedule) {
        descriptor.write(INFRASTRUCTURE + MOUNTER.formatted(scheduleLine(schedule)));
    }

    @Then("after a round that ends at {string} the {word} runs next at {string}")
    public void afterARoundTheModuleRunsNextAt(String roundEnd, String module, String next) {
        assertThat(scheduleOf(module).nextExecutionAfter(ZonedDateTime.parse(roundEnd)).map(ZonedDateTime::toInstant))
                .contains(ZonedDateTime.parse(next).toInstant());
    }

    @Then("the crawler runs when it starts")
    public void theCrawlerRunsWhenItStarts() {
        ZonedDateTime start = ZonedDateTime.now();
        assertThat(scheduleOf("crawler").firstExecution(start)).isEqualTo(start);
    }

    @Then("the crawler does not run again after its first round")
    public void theCrawlerDoesNotRunAgain() {
        assertThat(scheduleOf("crawler").nextExecutionAfter(ZonedDateTime.now())).isEmpty();
    }

    private Schedule scheduleOf(String module) {
        return SCHEDULE_OF_MODULE.get(module).apply(descriptor)
                .orElseThrow(() -> new AssertionError("the " + module + " has no schedule"));
    }

    private static String scheduleLine(String schedule) {
        return "      schedule: " + schedule + "\n";
    }

    private static <T> T module(DescriptorUnderTest descriptor, Class<T> type) {
        return descriptor.loaded().modules().stream().filter(type::isInstance).map(type::cast).findFirst()
                .orElseThrow(() -> new AssertionError("the descriptor has no " + type.getSimpleName()));
    }
}
