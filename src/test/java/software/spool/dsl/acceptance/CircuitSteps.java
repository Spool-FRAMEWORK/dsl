package software.spool.dsl.acceptance;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.dsl.acceptance.circuit.ScriptedHttpSource;
import software.spool.dsl.acceptance.circuit.ScriptedHttpSource.Response;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class CircuitSteps {
    private static final String ORDERS = "{\"orders\": [{\"id\": \"order-1\", \"amount\": 10}]}";
    private static final Duration MARGIN = Duration.ofMillis(400);
    private static final Duration WAIT_FOR_POLLS = Duration.ofSeconds(10);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    private final DescriptorUnderTest descriptor;
    private ScriptedHttpSource source;
    private Path inbox;
    private String crawler;
    private String sourceId;
    private int everySeconds;
    private final List<String> declared = new ArrayList<>();
    private int pollsBeforeTheWait = 1;

    public CircuitSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("an HTTP source {string} that answers with a list of orders")
    public void anHttpSourceThatAnswersWithAListOfOrders(String id) throws IOException {
        sourceId = id;
        source = new ScriptedHttpSource(ORDERS);
        inbox = Files.createTempDirectory("spool-circuit-inbox");
    }

    @Given("the descriptor declares the crawler {string} polling {string} every {int}s")
    public void theDescriptorDeclaresTheCrawler(String id, String polled, int seconds) {
        crawler = id;
        everySeconds = seconds;
        writeDescriptor();
    }

    @Given("the source of {string} declares:")
    public void theSourceDeclares(String id, String configuration) {
        configuration.lines().forEach(declared::add);
        writeDescriptor();
    }

    @Given("the source {string} answers {int} with the header {string} once")
    public void theSourceAnswersWithTheHeaderOnce(String id, int status, String header) {
        String[] nameAndValue = header.split(":", 2);
        source.answerOnce(() -> Response.withHeader(status, nameAndValue[0].strip(), nameAndValue[1].strip()));
    }

    @Given("the source {string} answers {int} with a {string} header {int}s ahead as an HTTP date once")
    public void theSourceAnswersWithAnHttpDateOnce(String id, int status, String header, int seconds) {
        answerOnceWithATimeAhead(status, header, seconds,
                time -> DateTimeFormatter.RFC_1123_DATE_TIME.format(time.atOffset(ZoneOffset.UTC)));
    }

    @Given("the source {string} answers {int} with a {string} header {int}s ahead in epoch seconds once")
    public void theSourceAnswersWithEpochSecondsOnce(String id, int status, String header, int seconds) {
        answerOnceWithATimeAhead(status, header, seconds, time -> String.valueOf(time.getEpochSecond()));
    }

    @Given("^the source \"([^\"]*)\" answers (\\d+) with the body (\\{.*\\}) once$")
    public void theSourceAnswersWithTheBodyOnce(String id, int status, String body) {
        source.answerOnce(() -> Response.withBody(status, body));
    }

    @Given("the source {string} answers {int} every time")
    public void theSourceAnswersEveryTime(String id, int status) {
        source.answerEveryTime(Response.status(status));
    }

    @Given("the source {string} answers {int} the first {int} times")
    public void theSourceAnswersTheFirstTimes(String id, int status, int times) {
        for (int i = 0; i < times; i++) source.answerOnce(() -> Response.status(status));
    }

    @When("the source {string} has been polled once")
    public void theSourceHasBeenPolledOnce(String id) {
        theSourceHasBeenPolledTimes(id, 1);
    }

    @When("the source {string} has been polled {int} times")
    public void theSourceHasBeenPolledTimes(String id, int times) {
        pollsBeforeTheWait = times;
        waitForPolls(times, Instant.now().plus(WAIT_FOR_POLLS));
    }

    @When("the descriptor is read")
    public void theDescriptorIsRead() {
        descriptor.load(Optional.empty());
    }

    @Then("the source {string} is not polled again for {int}s")
    public void theSourceIsNotPolledAgainFor(String id, int seconds) throws InterruptedException {
        Instant waitStarted = waitStarted();
        Instant quietUntil = waitStarted.plusSeconds(seconds).minus(MARGIN);
        Duration left = Duration.between(Instant.now(), quietUntil);
        if (!left.isNegative()) Thread.sleep(left.toMillis());

        assertThat(source.polls().stream().skip(pollsBeforeTheWait).filter(poll -> poll.isBefore(quietUntil)))
                .as("polls of %s within %ss of %s", sourceId, seconds, waitStarted)
                .isEmpty();
    }

    @Then("the source {string} is polled again after {int}s")
    public void theSourceIsPolledAgainAfter(String id, int seconds) {
        Instant waitStarted = waitStarted();
        waitForPolls(pollsBeforeTheWait + 1,
                waitStarted.plusSeconds(seconds + everySeconds).plus(WAIT_FOR_POLLS));

        assertThat(source.polls().get(pollsBeforeTheWait)).isAfter(waitStarted.plusSeconds(seconds).minus(MARGIN));
    }

    @Then("it is rejected with a message that names the source {string} and the key {string}")
    public void itIsRejectedNamingTheSourceAndTheKey(String id, String key) {
        assertThat(descriptor.failure().getMessage()).contains(id).contains(key);
    }

    @After
    public void stopTheSource() {
        if (source != null) source.close();
    }

    private void answerOnceWithATimeAhead(int status, String header, int seconds, Function<Instant, String> written) {
        source.answerOnce(() -> Response.withHeader(status, header,
                written.apply(Instant.now().plusSeconds(seconds).truncatedTo(ChronoUnit.SECONDS).plusSeconds(1))));
    }

    private Instant waitStarted() {
        waitForPolls(pollsBeforeTheWait, Instant.now().plus(WAIT_FOR_POLLS));
        return source.polls().get(pollsBeforeTheWait - 1);
    }

    private void waitForPolls(int count, Instant deadline) {
        while (source.polls().size() < count && Instant.now().isBefore(deadline)) {
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        assertThat(source.polls()).as("polls of %s", sourceId).hasSizeGreaterThanOrEqualTo(count);
    }

    private void writeDescriptor() {
        StringBuilder yaml = new StringBuilder()
                .append("infrastructure:\n")
                .append("  bus: {kind: EVENT_BUS, type: IN_MEMORY}\n")
                .append("  inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: \"")
                .append(inbox.toString().replace('\\', '/')).append("\"}}\n")
                .append("modules:\n")
                .append("  - crawler:\n")
                .append("      type: POLL\n")
                .append("      id: ").append(crawler).append('\n')
                .append("      schedule: {every: ").append(everySeconds).append("s}\n")
                .append("      source:\n")
                .append("        type: HTTP\n")
                .append("        normalizer: JSON_OBJECT\n")
                .append("        mediaType: JSON\n")
                .append("        configuration:\n")
                .append("          sourceId: ").append(sourceId).append('\n')
                .append("          url: \"").append(source.url()).append("\"\n");
        declared.forEach(line -> yaml.append("          ").append(line).append('\n'));
        yaml.append("      eventMapping:\n")
                .append("        namingConvention: SNAKE_CASE\n");
        descriptor.write(yaml.toString());
    }
}
