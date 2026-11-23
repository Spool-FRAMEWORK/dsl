package software.spool.dsl.acceptance;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.dsl.SpoolNodeDSL;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class WebhookSteps {
    private static final String SECRET_VARIABLE = "BOE_WEBHOOK_SECRET";
    private static final String SECRET = "a-secret-the-sender-and-spool-share";
    private static final String OTHER_SECRET = "a-secret-nobody-shares";
    private static final String SIGNATURE_HEADER = "X-Signature-256";
    private static final String TIMESTAMP_HEADER = "X-Signature-Timestamp";
    private static final String JSON = "application/json";
    private static final String RETRY_AFTER = "Retry-After";

    private final DescriptorUnderTest descriptor;
    private final HttpClient client = HttpClient.newHttpClient();
    private final List<HttpResponse<String>> answers = new ArrayList<>();
    private Path inbox;
    private int port;
    private String provider = "boe-sender";
    private String role = "boe-publisher";
    private Optional<String> webhookAuthentication = Optional.empty();
    private Optional<String> webhookRole = Optional.empty();
    private Optional<String> path = Optional.empty();
    private Optional<Integer> maxBodySize = Optional.empty();
    private String sourceId = "boe";
    private String inboxCircuit = "";
    private Optional<SpoolNode> node = Optional.empty();

    public WebhookSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("an inbox in a temporary folder of the file system")
    public void anInbox() throws IOException {
        inbox = Files.createTempDirectory("spool-inbox");
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
    }

    @Given("the secret of the {string} provider is in the environment")
    public void theSecretIsInTheEnvironment(String name) {
        descriptor.setEnvironmentVariable(SECRET_VARIABLE, SECRET);
    }

    @Given("the descriptor declares the provider {string}, which signs the body with a timestamp and gives the role {string}")
    public void theProviderSignsWithATimestamp(String name, String role) {
        this.provider = name;
        this.role = role;
        writeDescriptor();
    }

    @Given("the descriptor declares a webhook crawler for the source {string} at {string} that splits a JSON array")
    public void aWebhookCrawler(String source, String path) {
        this.sourceId = source;
        this.path = Optional.of(path);
        this.webhookAuthentication = Optional.of(provider);
        this.webhookRole = Optional.of(role);
        writeDescriptor();
    }

    @Given("the provider {string} gives the role {string} instead")
    public void theProviderGivesAnotherRole(String name, String role) {
        this.role = role;
        writeDescriptor();
    }

    @Given("the webhook accepts deliveries of at most {int} bytes")
    public void theWebhookAcceptsAtMost(int bytes) {
        maxBodySize = Optional.of(bytes);
        writeDescriptor();
    }

    @Given("the webhook crawler names no authentication provider")
    public void noAuthenticationProvider() {
        webhookAuthentication = Optional.empty();
        writeDescriptor();
    }

    @Given("the webhook crawler is authenticated by {string}")
    public void authenticatedBy(String name) {
        webhookAuthentication = Optional.of(name);
        writeDescriptor();
    }

    @Given("the webhook crawler asks for no role")
    public void noRole() {
        webhookRole = Optional.empty();
        writeDescriptor();
    }

    @Given("the webhook crawler has no path")
    public void noPath() {
        path = Optional.empty();
        writeDescriptor();
    }

    @Given("the inbox opens its circuit after {int} failed writes, for {int}s")
    public void theInboxOpensItsCircuitAfter(int failedWrites, int seconds) {
        inboxCircuit = ", circuitFailureRate: 100%, circuitMinimumCalls: " + failedWrites + ", circuitOpenFor: " + seconds + "s";
        writeDescriptor();
    }

    @Given("the inbox cannot be written")
    public void theInboxCannotBeWritten() throws IOException {
        deleteTheInbox();
        Files.createFile(inbox);
    }

    @Given("the inbox can be written again")
    public void theInboxCanBeWrittenAgain() throws IOException {
        Files.delete(inbox);
        Files.createDirectory(inbox);
    }

    @Given("^\"([^\"]*)\" has delivered (\\[.*]) and then (\\[.*]) signed with the secret$")
    public void hasDelivered(String sender, String first, String second) {
        deliver(first, JSON, Optional.of(SECRET), Instant.now());
        deliver(second, JSON, Optional.of(SECRET), Instant.now());
    }

    @When("{int} seconds have passed")
    public void secondsHavePassed(int seconds) throws InterruptedException {
        Thread.sleep(Duration.ofSeconds(seconds).toMillis());
    }

    @When("^\"([^\"]*)\" delivers (\\[.*]) signed with the secret$")
    public void deliversSigned(String sender, String body) {
        deliver(body, JSON, Optional.of(SECRET), Instant.now());
    }

    @When("^\"([^\"]*)\" delivers (\\[.*]) signed with the secret again$")
    public void deliversSignedAgain(String sender, String body) {
        deliver(body, JSON, Optional.of(SECRET), Instant.now());
    }

    @When("^someone delivers (\\[.*]) signed with another secret$")
    public void deliversWithAnotherSecret(String body) {
        deliver(body, JSON, Optional.of(OTHER_SECRET), Instant.now());
    }

    @When("^someone delivers (\\[.*]) without a signature$")
    public void deliversWithoutASignature(String body) {
        deliver(body, JSON, Optional.empty(), Instant.now());
    }

    @When("^\"([^\"]*)\" delivers (\\[.*]) signed with the secret (\\d+) minutes ago$")
    public void deliversSignedLongAgo(String sender, String body, int minutes) {
        deliver(body, JSON, Optional.of(SECRET), Instant.now().minus(Duration.ofMinutes(minutes)));
    }

    @When("{string} delivers a JSON array of {int} bytes signed with the secret")
    public void deliversABigArray(String sender, int bytes) {
        String records = IntStream.range(0, bytes).mapToObj(i -> "1").collect(Collectors.joining(","));
        String body = ("[" + records + "]").substring(0, bytes - 1) + "]";
        deliver(body, JSON, Optional.of(SECRET), Instant.now());
    }

    @When("{string} delivers a CSV body signed with the secret")
    public void deliversACsvBody(String sender) {
        deliver("id,title\na-1,Ley 1/2026\n", "text/csv", Optional.of(SECRET), Instant.now());
    }

    @Then("the delivery is accepted")
    public void theDeliveryIsAccepted() {
        assertThat(last().statusCode()).as(last().body()).isEqualTo(202);
    }

    @Then("both deliveries are accepted")
    public void bothDeliveriesAreAccepted() {
        assertThat(answers).hasSize(2).allSatisfy(answer -> assertThat(answer.statusCode()).as(answer.body()).isEqualTo(202));
    }

    @Then("the delivery is refused because the sender is not identified")
    public void refusedNotIdentified() {
        assertThat(last().statusCode()).isEqualTo(401);
    }

    @Then("the delivery is refused for lack of permission")
    public void refusedForLackOfPermission() {
        assertThat(last().statusCode()).isEqualTo(403);
    }

    @Then("the delivery is refused because it is too large")
    public void refusedTooLarge() {
        assertThat(last().statusCode()).isEqualTo(413);
    }

    @Then("the delivery is refused because its media type is not the one of the source")
    public void refusedMediaType() {
        assertThat(last().statusCode()).isEqualTo(415);
    }

    @Then("the delivery is answered 503 with a Retry-After of at most {int} seconds")
    public void answeredWithRetryAfter(int seconds) {
        assertThat(last().statusCode()).as(last().body()).isEqualTo(503);
        assertThat(last().headers().firstValue(RETRY_AFTER)).hasValueSatisfying(value ->
                assertThat(Integer.parseInt(value)).isBetween(1, seconds));
    }

    @Then("the delivery is answered 503 without a Retry-After")
    public void answeredWithoutRetryAfter() {
        assertThat(last().statusCode()).as(last().body()).isEqualTo(503);
        assertThat(last().headers().firstValue(RETRY_AFTER)).isEmpty();
    }

    @Then("the inbox has {int} captured envelope(s) of the source {string}")
    public void theInboxHasCapturedEnvelopes(int count, String source) {
        assertThat(captured()).hasSize(count)
                .allSatisfy(envelope -> assertThat(envelope.metadata().get(EventMetadataKey.SOURCE)).isEqualTo(source));
    }

    @Then("^the captured envelope holds (\\{.*})$")
    public void theCapturedEnvelopeHolds(String record) {
        assertThat(captured()).singleElement().satisfies(envelope ->
                assertThat(json(new String(envelope.payload(), StandardCharsets.UTF_8))).isEqualTo(json(record)));
    }

    @Then("the inbox is empty")
    public void theInboxIsEmpty() {
        assertThat(filesIn(inbox)).isEmpty();
    }

    @After
    public void stopTheNode() {
        node.ifPresent(SpoolNode::stop);
    }

    private void writeDescriptor() {
        StringBuilder yaml = new StringBuilder("server: {port: " + port + "}\n")
                .append("authentication:\n  ").append(provider).append(":\n    type: HMAC_SIGNATURE\n    configuration:\n")
                .append("      header: ").append(SIGNATURE_HEADER).append('\n')
                .append("      secret: \"${env:").append(SECRET_VARIABLE).append("}\"\n")
                .append("      timestampHeader: ").append(TIMESTAMP_HEADER).append('\n')
                .append("      tolerance: PT5M\n")
                .append("      roles: ").append(role).append('\n')
                .append("infrastructure:\n  bus: {kind: EVENT_BUS, type: IN_MEMORY}\n")
                .append("  inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: \"")
                .append(inbox.toString().replace('\\', '/')).append("\"").append(inboxCircuit).append("}}\n");
        if (webhookAuthentication.isEmpty() && webhookRole.isEmpty() && path.isEmpty()) {
            descriptor.write(yaml.append("modules: []\n").toString());
            return;
        }
        yaml.append("modules:\n  - crawler:\n      type: WEBHOOK\n      id: boe-webhook\n")
                .append("      source:\n        type: WEBHOOK\n        configuration:\n          sourceId: ").append(sourceId).append('\n');
        path.ifPresent(value -> yaml.append("          path: ").append(value).append('\n'));
        yaml.append("        normalizer: JSON_ARRAY\n        mediaType: JSON\n")
                .append("      eventMapping: {namingConvention: SNAKE_CASE}\n")
                .append("      webhook:\n");
        webhookAuthentication.ifPresent(value -> yaml.append("        authentication: ").append(value).append('\n'));
        webhookRole.ifPresent(value -> yaml.append("        role: ").append(value).append('\n'));
        maxBodySize.ifPresent(value -> yaml.append("        maxBodySize: ").append(value).append('\n'));
        descriptor.write(yaml.toString());
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

    private void deliver(String body, String contentType, Optional<String> secret, Instant signedAt) {
        startedNode();
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String timestamp = String.valueOf(signedAt.getEpochSecond());
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path.orElseThrow()))
                .header("Content-Type", contentType)
                .header(TIMESTAMP_HEADER, timestamp)
                .POST(HttpRequest.BodyPublishers.ofByteArray(bytes));
        secret.ifPresent(key -> request.header(SIGNATURE_HEADER, "sha256=" + signature(key, timestamp, bytes)));
        try {
            answers.add(client.send(request.build(), HttpResponse.BodyHandlers.ofString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void deleteTheInbox() throws IOException {
        try (Stream<Path> paths = Files.walk(inbox)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }

    private static String signature(String secret, String timestamp, byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            mac.update((timestamp + ".").getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(mac.doFinal(body));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private HttpResponse<String> last() {
        return answers.getLast();
    }

    private List<Envelope> captured() {
        return filesIn(inbox.resolve(EnvelopeStatus.CAPTURED.name())).stream().map(WebhookSteps::read).toList();
    }

    private static List<Path> filesIn(Path folder) {
        if (!Files.exists(folder)) return List.of();
        try (Stream<Path> files = Files.walk(folder)) {
            return files.filter(Files::isRegularFile).toList();
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

    private static Map<String, Object> json(String text) {
        return PayloadDeserializerFactory.json().asMap().deserialize(text.getBytes(StandardCharsets.UTF_8));
    }
}
