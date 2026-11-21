package software.spool.dsl.acceptance.normalizers;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import org.slf4j.LoggerFactory;
import software.spool.core.model.vo.MediaType;
import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writes a crawler around the source block of a scenario and runs the normalizer the descriptor composes on what the
 * source fetches. The source points nowhere: the scenarios check the normalizer, not the fetching.
 */
public class NormalizerCompositionSteps {
    private static final String CRAWLER = """
            infrastructure:
              eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
              inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/normalizer-inbox}}
            modules:
              - crawler:
                  type: POLL
                  id: composed-crawler
                  source:
                    type: HTTP
                    configuration: {sourceId: items, url: "http://localhost:8000/items"}
            %s
                  eventMapping:
                    namingConvention: SNAKE_CASE
            """;
    private static final String SOURCE_INDENT = "        ";

    private final DescriptorUnderTest descriptor;
    private final ListAppender<ILoggingEvent> log = new ListAppender<>();
    private byte[] fetched = new byte[0];
    private List<String> records = List.of();

    public NormalizerCompositionSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Before
    public void listenToTheLog() {
        log.start();
        rootLogger().addAppender(log);
    }

    @After
    public void stopListeningToTheLog() {
        rootLogger().detachAppender(log);
        log.stop();
    }

    @Given("the source fetches:")
    public void theSourceFetches(String payload) {
        fetched = payload.getBytes(StandardCharsets.UTF_8);
    }

    @Given("the source block:")
    public void theSourceBlock(String block) {
        descriptor.write(CRAWLER.formatted(block.indent(SOURCE_INDENT.length()).stripTrailing()));
    }

    @And("the source normalizes what it fetches")
    @SuppressWarnings("unchecked")
    public void theSourceNormalizesWhatItFetches() {
        Normalizer<byte[]> normalizer = (Normalizer<byte[]>) InfrastructurePluginFactory.normalizer(source());
        records = normalizer.normalize(fetched).map(record -> new String(record, StandardCharsets.UTF_8)).toList();
    }

    @Then("the records are:")
    public void theRecordsAre(List<String> expected) {
        assertThat(records).containsExactlyElementsOf(expected);
    }

    @Then("the log shows the normalizer of the source as:")
    public void theLogShowsTheNormalizerOfTheSourceAs(String normalizer) {
        assertThat(log.list).extracting(ILoggingEvent::getFormattedMessage)
                .anySatisfy(message -> assertThat(message).contains(normalizer));
    }

    @Then("the records of the source are labelled {string}")
    public void theRecordsOfTheSourceAreLabelled(String label) {
        assertThat(source().mediaType()).isEqualTo(MediaType.of(label));
    }

    private SourceDescriptor source() {
        return ((CrawlerDescriptor) descriptor.loaded().modules().getFirst()).source();
    }

    private static Logger rootLogger() {
        return (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    }
}
