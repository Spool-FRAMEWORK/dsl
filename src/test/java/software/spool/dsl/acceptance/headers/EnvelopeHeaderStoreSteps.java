package software.spool.dsl.acceptance.headers;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopeStored;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionKeySchema;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.dsl.SpoolNodeDSL;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.acceptance.quarantine.TestCallerRequestAuthenticatorProvider;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemDataLakeWriter;
import software.spool.infrastructure.adapter.inbox.filesystem.FileSystemInboxWriter;
import software.spool.infrastructure.adapter.quarantine.FileSystemQuarantineRepository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvelopeHeaderStoreSteps {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final String BRONZE_PARTITION = "year=2026/month=11/day=17/hour=10/source=orders/eventType=Order";
    private static final String CALLER = "ana";
    private static final String RESOLVE_ROLE = "quarantine-resolver";

    private final DescriptorUnderTest descriptor;
    private final TestEnvelopeHeaderStore store = TestEnvelopeHeaderStore.shared();
    private final List<IdempotencyKey> captured = new ArrayList<>();
    private Path inbox;
    private Path dataLake;
    private Path quarantine;
    private int port;
    private Optional<SpoolNode> node = Optional.empty();

    public EnvelopeHeaderStoreSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("an inbox, a data lake and a quarantine in temporary folders of the file system")
    public void anInboxADataLakeAndAQuarantine() throws IOException {
        inbox = Files.createTempDirectory("spool-inbox");
        dataLake = Files.createTempDirectory("spool-data-lake");
        quarantine = Files.createTempDirectory("spool-quarantine");
        port = freePort();
    }

    @Given("a store of envelope headers that the test can look into")
    public void aStoreOfEnvelopeHeaders() {
        store.clear();
    }

    @Given("the node declares the store of envelope headers and an ingester")
    public void theStoreAndAnIngester() {
        descriptor.write(infrastructure(Optional.of(TestEnvelopeHeaderStore.TYPE)) + ingester());
    }

    @Given("the node declares an ingester and no store of envelope headers")
    public void anIngesterWithoutAStore() {
        descriptor.write(infrastructure(Optional.empty()) + ingester());
    }

    @Given("the node declares the store of envelope headers and a mounter to silver that validates its records")
    public void theStoreAndAMounterToSilver() {
        descriptor.write(infrastructure(Optional.of(TestEnvelopeHeaderStore.TYPE)) + """
                modules:
                  - mounter:
                      id: orders-silver
                      type: POLL
                      schedule: {every: 1h}
                      mode: TRANSFORMATION
                      validate: true
                      read: {partitions: ["source=orders/eventType=Order"]}
                      write: {dataMart: orders, partitioning: {type: SAME_AS_SOURCE}}
                """);
    }

    @Given("the node declares the store of envelope headers and a janitor")
    public void theStoreAndAJanitor() {
        descriptor.write("server: {port: " + port + "}\n"
                + "authentication:\n  people: {type: " + TestCallerRequestAuthenticatorProvider.TYPE
                + ", configuration: {roles: \"" + RESOLVE_ROLE + "\"}}\n"
                + infrastructure(Optional.of(TestEnvelopeHeaderStore.TYPE)) + """
                modules:
                  - janitor:
                      id: orders-janitor
                      schedule: {every: 1h}
                      quarantineServer:
                        authentication: people
                        roles: {read: quarantine-reader, resolve: quarantine-resolver, bypass: quarantine-bypasser}
                """);
    }

    @Given("the node declares a store of envelope headers of type {string}")
    public void aStoreOfType(String type) {
        descriptor.write(infrastructure(Optional.of(type)) + ingester());
    }

    @Given("the inbox has the captured envelopes {string} and {string} of the source {string}")
    public void theInboxHasTheCapturedEnvelopes(String first, String second, String source) {
        FileSystemInboxWriter writer = new FileSystemInboxWriter(inbox.toString());
        for (String key : List.of(first, second)) {
            writer.receive(envelope(key, source, 10, EnvelopeStatus.CAPTURED));
            captured.add(IdempotencyKey.of(key));
        }
    }

    @Given("the data lake has the valid envelope {string} and the invalid envelope {string} of the source {string}, with their headers in the store")
    public void theDataLakeHasAValidAndAnInvalidEnvelope(String valid, String invalid, String source) {
        PartitionKey partition = partitionOf(BRONZE_PARTITION);
        List<PartitionedEnvelope> envelopes = List.of(
                new PartitionedEnvelope(partition, envelope(valid, source, 10, EnvelopeStatus.PERSISTED)),
                new PartitionedEnvelope(partition, envelope(invalid, source, -5, EnvelopeStatus.PERSISTED)));
        new FileSystemDataLakeWriter(dataLake.toString()).write(envelopes).toList();
        envelopes.forEach(item -> store.write(item.partition(), item.envelope().header()));
    }

    @Given("the quarantine has the envelope {string} of the source {string}, taken out of the data lake")
    public void theQuarantineHasTheEnvelope(String key, String source) {
        new FileSystemQuarantineRepository(quarantine.toString()).save(QuarantinedRecord.pending(
                envelope(key, source, -5, EnvelopeStatus.QUARANTINED), QuarantineLocation.bronze(partitionOf(BRONZE_PARTITION)),
                QuarantineReason.VALIDATION_FAILED, List.of("amount must not be negative"), Instant.now()));
    }

    @When("the ingester writes them to the data lake")
    public void theIngesterWritesThemToTheDataLake() {
        startTheNode();
        SpoolNodeDescriptor loaded = descriptor.loaded();
        var bus = InfrastructurePluginFactory.eventBus(InfrastructureDescriptor.unnamed(loaded.watchdog(), loaded.infrastructure()));
        captured.forEach(key -> bus.publish(EnvelopeStored.builder().idempotencyKey(key).build()));
        eventually(() -> assertThat(bronzeFileNames()).contains(captured.stream().map(key -> key.value() + ".json").toArray(String[]::new)));
    }

    @When("the mounter mounts once")
    public void theMounterMountsOnce() {
        startTheNode();
        eventually(() -> assertThat(filesUnder(quarantine)).isNotEmpty());
    }

    @When("the record {string} is republished from the quarantine")
    public void theRecordIsRepublished(String key) {
        startTheNode();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/spool/quarantine/" + key + "/republish"))
                .header(TestCallerRequestAuthenticatorProvider.CALLER_HEADER, CALLER)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<String> answer = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(answer.statusCode()).as(answer.body()).isEqualTo(200);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Then("the data lake has the envelopes {string} and {string}")
    public void theDataLakeHasTheEnvelopes(String first, String second) {
        eventually(() -> assertThat(bronzeFileNames()).contains(first + ".json", second + ".json"));
    }

    @Then("the data lake has the envelope {string}")
    public void theDataLakeHasTheEnvelope(String key) {
        eventually(() -> assertThat(bronzeFileNames()).contains(key + ".json"));
    }

    @Then("the data lake no longer has the envelope {string}")
    public void theDataLakeNoLongerHasTheEnvelope(String key) {
        eventually(() -> assertThat(bronzeFileNames()).doesNotContain(key + ".json"));
    }

    @Then("the store has the header of {string} in the partition of the source {string}")
    public void theStoreHasTheHeaderInThePartitionOfTheSource(String key, String source) {
        eventually(() -> assertThat(store.stored(IdempotencyKey.of(key))).hasValueSatisfying(stored ->
                assertThat(KeyEntries.split(stored.partition().value())).contains("source=" + source)));
    }

    @Then("each header is the one the data lake keeps for its envelope")
    public void eachHeaderIsTheOneTheDataLakeKeeps() {
        assertThat(store.all()).hasSize(captured.size())
                .allSatisfy(stored -> assertThat(stored.header()).isEqualTo(headerInTheDataLake(stored.header().idempotencyKey())));
    }

    @Then("the store has no headers")
    public void theStoreHasNoHeaders() {
        assertThat(store.all()).isEmpty();
    }

    @Then("the store no longer has the header of {string}")
    public void theStoreNoLongerHasTheHeader(String key) {
        eventually(() -> assertThat(store.stored(IdempotencyKey.of(key))).isEmpty());
    }

    @Then("the store still has the header of {string}")
    public void theStoreStillHasTheHeader(String key) {
        assertThat(store.stored(IdempotencyKey.of(key))).isPresent();
    }

    @Then("the error names the path {string}")
    public void theErrorNamesThePath(String path) {
        assertThat(descriptor.failure().getMessage()).contains(path);
    }

    @After
    public void stopTheNode() {
        node.ifPresent(SpoolNode::stop);
    }

    private void startTheNode() {
        descriptor.load(Optional.empty());
        SpoolNode started = SpoolNodeDSL.fromDescriptor(descriptor.loaded());
        try {
            started.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        node = Optional.of(started);
    }

    private String infrastructure(Optional<String> headersType) {
        StringBuilder yaml = new StringBuilder("infrastructure:\n  bus: {kind: EVENT_BUS, type: IN_MEMORY}\n")
                .append(piece("inbox", "INBOX", inbox))
                .append(piece("lake", "DATA_LAKE", dataLake))
                .append(piece("quarantine", "QUARANTINE_REPOSITORY", quarantine));
        headersType.ifPresent(type -> yaml.append("  headers: {kind: ENVELOPE_HEADERS, type: ").append(type).append("}\n"));
        return yaml.toString();
    }

    private static String ingester() {
        return """
                modules:
                  - ingester:
                      id: orders-ingester
                      type: REACTIVE
                """;
    }

    private static String piece(String name, String kind, Path path) {
        return "  " + name + ": {kind: " + kind + ", type: FILE_SYSTEM, configuration: {path: \""
                + path.toString().replace('\\', '/') + "\"}}\n";
    }

    private static Envelope envelope(String key, String source, long amount, EnvelopeStatus status) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", key);
        fields.put("amount", amount);
        EventMetadata metadata = new EventMetadata()
                .set(EventMetadataKey.SOURCE, source)
                .set(EventMetadataKey.PARTITION_SCHEMA, RecordSerializerFactory.record().serialize(PartitionKeySchema.of(source, Order.class)));
        return new Envelope(IdempotencyKey.of(key), metadata, MediaType.of("application/json"),
                RecordSerializerFactory.record().serialize(fields), status, 0, Instant.now(), Instant.now());
    }

    private static PartitionKey partitionOf(String path) {
        return new PartitionKey(KeyEntries.join(List.of(path.split("/"))));
    }

    private EnvelopeHeader headerInTheDataLake(IdempotencyKey key) {
        Path headerFile = filesUnder(dataLake.resolve("bronze")).stream()
                .filter(file -> file.getFileName().toString().equals(key.value() + ".meta.json"))
                .findFirst().orElseThrow();
        try {
            return PayloadDeserializerFactory.json().as(EnvelopeHeader.class).deserialize(Files.readAllBytes(headerFile));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> bronzeFileNames() {
        return filesUnder(dataLake.resolve("bronze")).stream().map(file -> file.getFileName().toString()).toList();
    }

    private static List<Path> filesUnder(Path root) {
        if (!Files.exists(root)) return List.of();
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void eventually(Runnable assertion) {
        Instant deadline = Instant.now().plus(TIMEOUT);
        while (true) {
            try {
                assertion.run();
                return;
            } catch (AssertionError e) {
                if (Instant.now().isAfter(deadline)) throw e;
                try {
                    Thread.sleep(POLL_INTERVAL.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
