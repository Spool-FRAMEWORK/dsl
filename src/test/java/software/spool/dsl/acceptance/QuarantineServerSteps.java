package software.spool.dsl.acceptance;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantineResolution;
import software.spool.core.model.quarantine.QuarantineStatus;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.quarantine.Resolver;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.dsl.SpoolNodeDSL;
import software.spool.dsl.acceptance.quarantine.TestCallerRequestAuthenticatorProvider;
import software.spool.infrastructure.adapter.quarantine.FileSystemQuarantineRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class QuarantineServerSteps {
    private static final String PROVIDER = "people";
    private static final String BASE = "/spool/quarantine";
    private static final String READ_ROLE = "quarantine-reader";
    private static final String RESOLVE_ROLE = "quarantine-resolver";
    private static final String BYPASS_ROLE = "quarantine-bypasser";
    private static final String JSON_OBJECT = "(\\{.*})";

    private final DescriptorUnderTest descriptor;
    private final HttpClient client = HttpClient.newHttpClient();
    private final Map<String, HttpResponse<String>> answers = new HashMap<>();
    private final List<HttpResponse<String>> simultaneous = new ArrayList<>();
    private Path inbox;
    private Path quarantine;
    private int port;
    private List<String> roles = List.of();
    private Optional<String> serverAuthentication = Optional.empty();
    private Optional<Integer> maxPageSize = Optional.empty();
    private Optional<SpoolNode> node = Optional.empty();
    private Optional<HttpResponse<String>> last = Optional.empty();
    private Optional<Envelope> newEnvelope = Optional.empty();

    public QuarantineServerSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("an inbox and a quarantine in temporary folders of the file system")
    public void anInboxAndAQuarantine() throws IOException {
        inbox = Files.createTempDirectory("spool-inbox");
        quarantine = Files.createTempDirectory("spool-quarantine");
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
    }

    @Given("the descriptor declares the provider {string}, which gives its callers the roles:")
    public void theProviderGivesTheRoles(String provider, DataTable roles) {
        this.roles = roles.asList();
        writeDescriptor();
    }

    @Given("the provider {string} only gives its callers the role {string}")
    public void theProviderOnlyGivesTheRole(String provider, String role) {
        this.roles = List.of(role);
        writeDescriptor();
    }

    @Given("the provider {string} only gives its callers the roles:")
    public void theProviderOnlyGivesTheRoles(String provider, DataTable roles) {
        this.roles = roles.asList();
        writeDescriptor();
    }

    @Given("the descriptor declares a janitor with a quarantine server authenticated by {string}")
    public void aJanitorWithAQuarantineServer(String provider) {
        serverAuthentication = Optional.of(provider);
        writeDescriptor();
    }

    @Given("the descriptor declares a janitor without a quarantine server")
    public void aJanitorWithoutAQuarantineServer() {
        serverAuthentication = Optional.empty();
        writeDescriptor();
    }

    @Given("the quarantine server lists at most {int} records per page")
    public void theServerListsAtMost(int records) {
        maxPageSize = Optional.of(records);
        writeDescriptor();
    }

    @Given("these envelopes are in quarantine, waiting to be resolved:")
    public void theseEnvelopesAreInQuarantine(DataTable envelopes) {
        FileSystemQuarantineRepository repository = new FileSystemQuarantineRepository(quarantine.toString());
        for (Map<String, String> row : envelopes.asMaps()) {
            repository.save(QuarantinedRecord.pending(envelope(row.get("envelope"), row.get("payload")), QuarantineLocation.inbox(),
                    QuarantineReason.VALIDATION_FAILED, List.of("amount must not be negative"), Instant.now()));
        }
    }

    @Given("{string} has purged {string}")
    public void hasPurged(String caller, String key) {
        purges(caller, key);
    }

    @When("{string} purges {string}")
    public void purges(String caller, String key) {
        answers.put("purge", post(caller, key, "purge", ""));
    }

    @When("{string} lists the records waiting to be resolved, {int} per page")
    public void lists(String caller, int perPage) {
        answers.put("list", call(Optional.of(caller), get(BASE + "?status=PENDING&limit=" + perPage)));
    }

    @When("{string} asks for the next page")
    public void asksForTheNextPage(String caller) {
        String next = String.valueOf(body().get("next"));
        call(Optional.of(caller), get(BASE + "?status=PENDING&limit=2&cursor=" + URLEncoder.encode(next, StandardCharsets.UTF_8)));
    }

    @When("{string} looks at the record {string}")
    public void looksAtTheRecord(String caller, String key) {
        call(Optional.of(caller), get(BASE + "/" + key));
    }

    @When("{string} republishes {string}")
    public void republishes(String caller, String key) {
        answers.put("republish", post(caller, key, "republish", ""));
    }

    @When("^\"([^\"]*)\" amends \"([^\"]*)\" with the payload " + JSON_OBJECT + "$")
    public void amends(String caller, String key, String payload) {
        post(caller, key, "amend", payload);
    }

    @When("{string} amends {string} with an empty payload")
    public void amendsWithAnEmptyPayload(String caller, String key) {
        post(caller, key, "amend", "");
    }

    @When("{string} lets {string} through because {string}")
    public void letsThrough(String caller, String key, String reason) {
        answers.put("bypass", post(caller, key, "bypass", "{\"reason\":\"" + reason + "\"}"));
    }

    @When("{string} lets {string} through without a reason")
    public void letsThroughWithoutAReason(String caller, String key) {
        post(caller, key, "bypass", "{}");
    }

    @When("{string} and {string} resolve {string} at the same time")
    public void resolveAtTheSameTime(String first, String second, String key) {
        startedNode();
        CompletableFuture<HttpResponse<String>> republish = CompletableFuture.supplyAsync(() -> post(first, key, "republish", ""));
        CompletableFuture<HttpResponse<String>> purge = CompletableFuture.supplyAsync(() -> post(second, key, "purge", ""));
        simultaneous.add(republish.join());
        simultaneous.add(purge.join());
    }

    @When("someone without a valid signature lists the records waiting to be resolved")
    public void someoneUnidentifiedLists() {
        call(Optional.empty(), get(BASE));
    }

    @Then("the page has {string} and {string}")
    public void thePageHas(String first, String second) {
        assertThat(keysOfThePage()).containsExactly(first, second);
    }

    @Then("the page has {string}")
    public void thePageHas(String only) {
        assertThat(keysOfThePage()).containsExactly(only);
    }

    @Then("the answer says there is a next page")
    public void thereIsANextPage() {
        assertThat(body().get("next")).isNotNull();
    }

    @Then("the answer says there is no next page")
    public void thereIsNoNextPage() {
        assertThat(body().get("next")).isNull();
    }

    @Then("^the answer shows its payload " + JSON_OBJECT + "$")
    @SuppressWarnings("unchecked")
    public void theAnswerShowsItsPayload(String payload) {
        Map<String, Object> envelope = (Map<String, Object>) body().get("envelope");
        assertThat(json(String.valueOf(envelope.get("payload")))).isEqualTo(json(payload));
    }

    @Then("the answer shows one stay in quarantine, still waiting to be resolved")
    @SuppressWarnings("unchecked")
    public void oneStayStillWaiting() {
        List<Map<String, Object>> history = (List<Map<String, Object>>) body().get("history");
        assertThat(history).singleElement().satisfies(stay ->
                assertThat(((Map<String, Object>) stay.get("resolution")).get("status")).isEqualTo("PENDING"));
    }

    @Then("^\"([^\"]*)\" is captured again in the inbox with the payload " + JSON_OBJECT + "$")
    public void isCapturedAgain(String key, String payload) {
        Envelope captured = inInbox(EnvelopeStatus.CAPTURED).stream()
                .filter(envelope -> envelope.idempotencyKey().value().equals(key)).findFirst().orElseThrow();
        assertThat(json(new String(captured.payload(), StandardCharsets.UTF_8))).isEqualTo(json(payload));
    }

    @Then("^a new envelope with the payload " + JSON_OBJECT + " is captured in the inbox$")
    public void aNewEnvelopeIsCaptured(String payload) {
        newEnvelope = inInbox(EnvelopeStatus.CAPTURED).stream()
                .filter(envelope -> json(new String(envelope.payload(), StandardCharsets.UTF_8)).equals(json(payload)))
                .findFirst();
        assertThat(newEnvelope).isPresent();
    }

    @Then("the new envelope says it comes from {string}")
    public void theNewEnvelopeComesFrom(String key) {
        assertThat(newEnvelope.orElseThrow().metadata().get(EventMetadataKey.CORRECTED_FROM)).isEqualTo(key);
    }

    @Then("the history of {string} says it was republished by {string}")
    public void republishedBy(String key, String caller) {
        assertThat(resolutionOf(key)).isInstanceOfSatisfying(QuarantineResolution.Republished.class,
                republished -> assertThat(republished.by()).isEqualTo(Resolver.identified(caller, PROVIDER)));
    }

    @Then("the history of {string} says it was corrected by {string}")
    public void correctedBy(String key, String caller) {
        assertThat(resolutionOf(key)).isInstanceOfSatisfying(QuarantineResolution.Corrected.class,
                corrected -> assertThat(corrected.by()).isEqualTo(Resolver.identified(caller, PROVIDER)));
    }

    @Then("the history of {string} says it was bypassed by {string} because {string}")
    public void bypassedBy(String key, String caller, String reason) {
        assertThat(resolutionOf(key)).isInstanceOfSatisfying(QuarantineResolution.Bypassed.class, bypassed -> {
            assertThat(bypassed.by()).isEqualTo(Resolver.identified(caller, PROVIDER));
            assertThat(bypassed.reason()).isEqualTo(reason);
        });
    }

    @Then("the history of {string} says it was purged by {string}")
    public void purgedBy(String key, String caller) {
        assertThat(resolutionOf(key)).isInstanceOfSatisfying(QuarantineResolution.Purged.class,
                purged -> assertThat(purged.by()).isEqualTo(Resolver.identified(caller, PROVIDER)));
    }

    @Then("the answer says the reason is missing")
    public void theReasonIsMissing() {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(400);
        assertThat(errorOf(last.orElseThrow())).contains("reason is missing");
    }

    @Then("the answer says the payload is missing")
    public void thePayloadIsMissing() {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(400);
        assertThat(errorOf(last.orElseThrow())).contains("payload is missing");
    }

    @Then("{string} is still waiting to be resolved")
    public void isStillWaiting(String key) {
        assertThat(recordOf(key).status()).isEqualTo(QuarantineStatus.PENDING);
    }

    @Then("the answer says {string} is not in quarantine")
    public void isNotInQuarantine(String key) {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(404);
        assertThat(errorOf(last.orElseThrow())).isEqualTo(key + " is not in quarantine");
    }

    @Then("the answer says {string} was already resolved")
    public void wasAlreadyResolved(String key) {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(409);
        assertThat(errorOf(last.orElseThrow())).startsWith(key + " was already resolved");
    }

    @Then("one of them resolves it")
    public void oneOfThemResolvesIt() {
        assertThat(simultaneous).filteredOn(answer -> answer.statusCode() == 200).hasSize(1);
    }

    @Then("the other is told {string} was already resolved")
    public void theOtherIsTold(String key) {
        assertThat(simultaneous).filteredOn(answer -> answer.statusCode() == 409).singleElement()
                .satisfies(answer -> assertThat(errorOf(answer)).startsWith(key + " was already resolved"));
    }

    @Then("the history of {string} has a single resolution")
    public void aSingleResolution(String key) {
        QuarantinedRecord quarantinedRecord = recordOf(key);
        assertThat(quarantinedRecord.history()).hasSize(1);
        assertThat(quarantinedRecord.status()).isNotEqualTo(QuarantineStatus.PENDING);
    }

    @Then("the answer says a page has at most {int} records")
    public void aPageHasAtMost(int records) {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(400);
        assertThat(errorOf(last.orElseThrow())).contains("at most " + records + " records");
    }

    @Then("the list is shown")
    public void theListIsShown() {
        assertThat(answers.get("list").statusCode()).isEqualTo(200);
    }

    @Then("the republish is refused for lack of permission")
    public void theRepublishIsRefused() {
        assertThat(answers.get("republish").statusCode()).isEqualTo(403);
    }

    @Then("the bypass is refused for lack of permission")
    public void theBypassIsRefused() {
        assertThat(answers.get("bypass").statusCode()).isEqualTo(403);
    }

    @Then("the call is refused because the caller is not identified")
    public void theCallerIsNotIdentified() {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(401);
    }

    @Then("the node does not know that address")
    public void theNodeDoesNotKnowThatAddress() {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(404);
    }

    @After
    public void stopTheNode() {
        node.ifPresent(SpoolNode::stop);
    }

    private void writeDescriptor() {
        StringBuilder yaml = new StringBuilder("server: {port: " + port + "}\n")
                .append("authentication:\n  ").append(PROVIDER).append(": {type: ").append(TestCallerRequestAuthenticatorProvider.TYPE)
                .append(", configuration: {roles: \"").append(String.join(",", roles)).append("\"}}\n")
                .append("infrastructure:\n  bus: {kind: EVENT_BUS, type: IN_MEMORY}\n")
                .append(piece("inbox", "INBOX", inbox))
                .append(piece("quarantine", "QUARANTINE_REPOSITORY", quarantine))
                .append("modules:\n  - janitor:\n      id: orders-janitor\n      schedule: {every: 1h}\n");
        serverAuthentication.ifPresent(provider -> {
            yaml.append("      quarantineServer:\n        authentication: ").append(provider).append('\n')
                    .append("        roles: {read: ").append(READ_ROLE).append(", resolve: ").append(RESOLVE_ROLE)
                    .append(", bypass: ").append(BYPASS_ROLE).append("}\n");
            maxPageSize.ifPresent(size -> yaml.append("        pageSize: ").append(size).append('\n')
                    .append("        maxPageSize: ").append(size).append('\n'));
        });
        descriptor.write(yaml.toString());
    }

    private static String piece(String name, String kind, Path path) {
        return "  " + name + ": {kind: " + kind + ", type: FILE_SYSTEM, configuration: {path: \""
                + path.toString().replace('\\', '/') + "\"}}\n";
    }

    private synchronized void startedNode() {
        if (node.isPresent()) return;
        descriptor.load(Optional.empty());
        SpoolNode started = SpoolNodeDSL.fromDescriptor(descriptor.loaded());
        try {
            started.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        node = Optional.of(started);
    }

    private HttpRequest.Builder get(String path) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET();
    }

    private HttpResponse<String> post(String caller, String key, String operation, String body) {
        return call(Optional.of(caller), HttpRequest.newBuilder(URI.create("http://localhost:" + port + BASE + "/" + key + "/" + operation))
                .POST(HttpRequest.BodyPublishers.ofString(body)));
    }

    private HttpResponse<String> call(Optional<String> caller, HttpRequest.Builder request) {
        startedNode();
        caller.ifPresent(name -> request.header(TestCallerRequestAuthenticatorProvider.CALLER_HEADER, name));
        try {
            HttpResponse<String> answer = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
            synchronized (this) {
                last = Optional.of(answer);
            }
            return answer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private Map<String, Object> body() {
        return json(last.orElseThrow().body());
    }

    @SuppressWarnings("unchecked")
    private List<String> keysOfThePage() {
        assertThat(last.orElseThrow().statusCode()).isEqualTo(200);
        return ((List<Map<String, Object>>) body().get("records")).stream().map(found -> String.valueOf(found.get("key"))).toList();
    }

    private static String errorOf(HttpResponse<String> answer) {
        return String.valueOf(json(answer.body()).get("error"));
    }

    private static Map<String, Object> json(String text) {
        return PayloadDeserializerFactory.json().asMap().deserialize(text.getBytes(StandardCharsets.UTF_8));
    }

    private QuarantinedRecord recordOf(String key) {
        return new FileSystemQuarantineRepository(quarantine.toString()).find(IdempotencyKey.of(key)).orElseThrow();
    }

    private QuarantineResolution resolutionOf(String key) {
        return recordOf(key).resolution();
    }

    private List<Envelope> inInbox(EnvelopeStatus status) {
        Path folder = inbox.resolve(status.name());
        if (!Files.exists(folder)) return List.of();
        try (Stream<Path> files = Files.list(folder)) {
            return files.map(QuarantineServerSteps::read).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Envelope read(Path file) {
        try {
            return PayloadDeserializerFactory.json().as(Envelope.class).deserialize(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Envelope envelope(String key, String payload) {
        return new Envelope(IdempotencyKey.of(key), new EventMetadata().set(EventMetadataKey.SOURCE, "orders"), MediaTypes.JSON,
                payload.getBytes(StandardCharsets.UTF_8), EnvelopeStatus.QUARANTINED, 0, Instant.now(), Instant.now());
    }
}
