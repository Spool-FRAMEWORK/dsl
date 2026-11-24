package software.spool.dsl.acceptance;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.event.EnvelopePersisted;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.MediaType;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.model.vo.PartitionedEnvelope;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.port.quarantine.QuarantinedResultWriter;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.acceptance.mounter.Payment;
import software.spool.dsl.providers.mounter.QuarantinedResultWriters;
import software.spool.dsl.registry.ModuleProviderRegistry;
import software.spool.infrastructure.adapter.dataLake.filesystem.FileSystemDataLakeWriter;
import software.spool.infrastructure.adapter.datamart.FileSystemDataMartWriter;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.validator.engine.ValidatorRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class MounterSteps {
    private static final Duration MOUNT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final String BRONZE_DATE_ENTRIES = "year=2026/month=11/day=17/hour=10";

    private final DescriptorUnderTest descriptor;
    private Path dataLake;
    private Path dataMart;
    private Path quarantine;
    private Optional<Path> aggregateMetadata = Optional.empty();
    private final Set<String> mounted = new HashSet<>();
    private boolean withDataLake = true;
    private boolean withDataMart = true;
    private boolean withQuarantine = true;
    private final Map<String, Path> dataMarts = new LinkedHashMap<>();
    private final StringBuilder mounters = new StringBuilder();
    private Optional<SpoolNode> node = Optional.empty();
    private int nodePort;
    private int silverFilesBeforeStart;

    public MounterSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("a data lake and a data mart in temporary folders of the file system")
    public void aDataLakeAndADataMart() throws IOException {
        dataLake = Files.createTempDirectory("spool-data-lake");
        dataMart = Files.createTempDirectory("spool-data-mart");
    }

    @Given("a quarantine in a temporary folder of the file system")
    public void aQuarantine() throws IOException {
        quarantine = Files.createTempDirectory("spool-quarantine");
    }

    @Given("aggregate metadata in a temporary folder of the file system")
    public void aggregateMetadataInATemporaryFolder() throws IOException {
        aggregateMetadata = Optional.of(Files.createTempDirectory("spool-aggregates"));
    }

    @Given("the descriptor has no quarantine")
    public void theDescriptorHasNoQuarantine() {
        withQuarantine = false;
    }

    @Given("the descriptor has no data lake")
    public void theDescriptorHasNoDataLake() {
        withDataLake = false;
    }

    @Given("the descriptor has no data mart")
    public void theDescriptorHasNoDataMart() {
        withDataMart = false;
    }

    @Given("the descriptor has the data marts {string} and {string} in temporary folders of the file system")
    public void theDescriptorHasTheDataMarts(String first, String second) throws IOException {
        withDataMart = false;
        dataMarts.put(first, Files.createTempDirectory("spool-data-mart-" + first));
        dataMarts.put(second, Files.createTempDirectory("spool-data-mart-" + second));
    }

    @Given("the payments source rejects a negative amount")
    public void thePaymentsSourceRejectsANegativeAmount() {
        assertThat(new ValidatorRegistry().hasValidatorsFor("payments", Payment.class)).isTrue();
    }

    @Given("bronze has these {string} records in the partition {string}:")
    public void bronzeHasTheseRecords(String source, String partition, DataTable records) {
        PartitionKey partitionKey = partitionOf(BRONZE_DATE_ENTRIES + "/" + partition);
        new FileSystemDataLakeWriter(dataLake.toString()).write(records.asMaps().stream()
                .map(record -> new PartitionedEnvelope(partitionKey, envelope(source, record)))
                .toList()).toList();
    }

    @Given("silver has these {string} records in the data mart {string} and the partition {string}:")
    public void silverHasTheseRecords(String source, String dataMartName, String partition, DataTable records) {
        PartitionKey partitionKey = partitionOf(partition);
        new FileSystemDataMartWriter(dataLake).write(MountTarget.transformation(dataMartName, partitionKey),
                records.asMaps().stream().map(record -> new PartitionedRecord<>(partitionKey, silverRecord(envelope(source, record)))));
    }

    @Given("silver has these {string} records in the data mart {string} and the partition of the previous hour:")
    public void silverHasTheseRecordsInThePartitionOfThePreviousHour(String source, String dataMartName, DataTable records) {
        ZonedDateTime previousHour = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        String partition = String.format("year=%d/month=%02d/day=%02d/hour=%02d", previousHour.getYear(),
                previousHour.getMonthValue(), previousHour.getDayOfMonth(), previousHour.getHour());
        silverHasTheseRecordsInTheClosedPartition(source, dataMartName, partition, records);
    }

    @Given("silver has these {string} records in the data mart {string} and the closed partition {string}:")
    public void silverHasTheseRecordsInTheClosedPartition(String source, String dataMartName, String partition, DataTable records) {
        silverHasTheseRecords(source, dataMartName, partition + "/source=" + source, records);
    }

    @Given("the descriptor declares the mounter {string}:")
    public void theDescriptorDeclaresTheMounter(String id, String mounter) {
        declareMounter(id, "", mounter);
    }

    @Given("the descriptor declares the mounter {string} that uses {string}:")
    public void theDescriptorDeclaresTheMounterThatUses(String id, String piece, String mounter) {
        declareMounter(id, "      uses: [" + piece + "]\n", mounter);
    }

    @Given("the descriptor declares the mounter {string} that uses the aggregate metadata:")
    public void theDescriptorDeclaresTheMounterThatUsesTheAggregateMetadata(String id, String mounter) {
        declareMounter(id, "      uses: [aggregates]\n", mounter);
    }

    @When("the node is started from the descriptor")
    public void theNodeIsStartedFromTheDescriptor() throws IOException {
        descriptor.load(Optional.empty());
        if (descriptor.hasLoaded()) {
            silverFilesBeforeStart = dataLake == null ? 0 : filesUnder(silver()).size();
            node = Optional.of(startedNode(descriptor.loaded()));
        }
    }

    @When("the mounter {string} mounts once")
    public void theMounterMountsOnce(String id) throws IOException {
        descriptor.loaded();
        if (!mounted.add(id)) {
            restartTheNode();
            return;
        }
        eventually(() -> assertThat(filesUnder(silver()).size() > silverFilesBeforeStart || goldWasWritten())
                .as("files written by " + id).isTrue());
    }

    @Given("the record {string} leaves bronze")
    public void theRecordLeavesBronze(String record) throws IOException {
        for (Path file : filesUnder(dataLake.resolve("bronze"))) {
            if (file.getFileName().toString().startsWith(record + ".")) Files.delete(file);
        }
    }

    @When("the ingester reports that it wrote the partition {string}")
    public void theIngesterReportsThatItWroteThePartition(String partition) {
        SpoolNodeDescriptor loaded = descriptor.loaded();
        InfrastructurePluginFactory.eventBus(InfrastructureDescriptor.unnamed(loaded.watchdog(), loaded.infrastructure())).publish(EnvelopePersisted.builder()
                .idempotencyKey(IdempotencyKey.of("reported-" + partition))
                .partitionKey(partitionOf(BRONZE_DATE_ENTRIES + "/" + partition))
                .build());
    }

    @When("the result of {string} with the id {string} and the amount {long} is taken out of the quarantine in the partition {string}")
    public void theResultIsTakenOutOfTheQuarantine(String dataMartName, String id, long amount, String partition) {
        QuarantinedResultWriter writer = QuarantinedResultWriters.of(descriptor.loaded()).create().stream()
                .filter(candidate -> candidate.dataMart().equals(dataMartName))
                .findFirst().orElseThrow();
        Envelope envelope = envelope("payments", Map.of("id", id, "amount", String.valueOf(amount)));
        writer.write(partitionOf(partition), RecordSerializerFactory.record().serialize(silverRecord(envelope).toMap()));
    }

    @Then("silver has the records {string} and {string} in the data mart {string}")
    public void silverHasTheRecords(String first, String second, String dataMartName) {
        eventually(() -> assertThat(contentUnder(silver().resolve(dataMartName))).contains(first, second));
    }

    @Then("silver has the record {string} in the data mart {string}")
    public void silverHasTheRecord(String record, String dataMartName) {
        eventually(() -> assertThat(contentUnder(silver().resolve(dataMartName))).contains(record));
    }

    @Then("silver has the record {string} in the partition {string} of the data mart {string}")
    public void silverHasTheRecordInThePartition(String record, String partition, String dataMartName) {
        eventually(() -> assertThat(contentUnder(silver().resolve(dataMartName).resolve(partition))).contains(record));
    }

    @Then("silver does not have the record {string}")
    public void silverDoesNotHaveTheRecord(String record) {
        eventually(() -> assertThat(contentUnder(silver())).doesNotContain(record));
    }

    @Then("silver keeps each record in the partition it had in bronze")
    public void silverKeepsEachRecordInThePartitionItHadInBronze() {
        Path bronzePartition = Path.of("year=2026", "month=11", "day=17", "hour=10", "source=payments", "eventType=Payment");
        Path silverDataMart = silver().resolve("payments");
        assertThat(filesUnder(silverDataMart)).isNotEmpty()
                .allSatisfy(file -> assertThat(silverDataMart.relativize(file.getParent())).isEqualTo(bronzePartition));
    }

    @Then("bronze no longer has the record {string}")
    public void bronzeNoLongerHasTheRecord(String record) {
        eventually(() -> assertThat(filesUnder(dataLake.resolve("bronze")))
                .noneMatch(file -> file.getFileName().toString().equals(record + ".json")));
    }

    @Then("gold has a single record with the total amount {long} in the data mart {string}")
    public void goldHasASingleRecordWithTheTotalAmount(long total, String dataMartName) {
        eventually(() -> {
            List<Path> files = filesUnder(gold().resolve(dataMartName));
            assertThat(files).hasSize(1);
            assertThat(read(files.getFirst())).contains("\"totalAmount\":" + total);
        });
    }

    @Then("the data mart {string} has a single record with the total amount {long} in {string}")
    public void theDataMartHasASingleRecordWithTheTotalAmount(String piece, long total, String dataMartName) {
        eventually(() -> {
            List<Path> files = filesUnder(dataMarts.get(piece).resolve("gold").resolve(dataMartName));
            assertThat(files).hasSize(1);
            assertThat(read(files.getFirst())).contains("\"totalAmount\":" + total);
        });
    }

    @Then("the data mart {string} has nothing in {string}")
    public void theDataMartHasNothingIn(String piece, String dataMartName) {
        assertThat(filesUnder(dataMarts.get(piece).resolve("gold").resolve(dataMartName))).isEmpty();
    }

    @Then("the aggregate metadata of {string} names the two partitions of silver")
    public void theAggregateMetadataNamesTheTwoPartitionsOfSilver(String dataMartName) {
        Path aggregates = aggregateMetadata.orElseThrow().resolve("gold").resolve(dataMartName).resolve("aggregates");
        eventually(() -> assertThat(filesUnder(aggregates)).singleElement().satisfies(file ->
                assertThat((List<?>) PayloadDeserializerFactory.json().asMap().deserialize(read(file).getBytes()).get("sources"))
                        .hasSize(2)));
    }

    @Then("the record {string} is in the quarantine")
    public void theRecordIsInTheQuarantine(String record) {
        eventually(() -> assertThat(filesUnder(quarantine).stream().map(MounterSteps::read))
                .anySatisfy(content -> assertThat(content).contains(record)));
    }

    @Then("the descriptor is rejected")
    public void theDescriptorIsRejected() {
        descriptor.failure();
    }

    @Then("the error names the module {string}")
    public void theErrorNamesTheModule(String id) {
        assertThat(descriptor.failure().getMessage()).contains(id);
    }

    @Then("the error says {string}")
    public void theErrorSays(String reason) {
        assertThat(descriptor.failure().getMessage()).contains(reason);
    }

    @After
    public void stopTheNode() {
        node.ifPresent(SpoolNode::stop);
    }

    /** A POLL mounter has its first round when it starts, so starting the node again gives it one more round. */
    private void restartTheNode() throws IOException {
        node.ifPresent(SpoolNode::stop);
        node = Optional.of(startedNode(descriptor.loaded()));
    }

    Path dataLakeFolder() {
        return dataLake;
    }

    int nodePort() {
        descriptor.loaded();
        return nodePort;
    }

    private SpoolNode startedNode(SpoolNodeDescriptor loaded) throws IOException {
        nodePort = freePort();
        SpoolNode started = SpoolNode.create(nodePort);
        loaded.declaredModules().forEach(module ->
                started.register(ModuleProviderRegistry.build(module.descriptor(), loaded.infrastructureOf(module))));
        started.start();
        return started;
    }

    private void declareMounter(String id, String uses, String mounter) {
        mounters.append("  - mounter:\n      id: ").append(id).append('\n').append(uses);
        mounter.lines().forEach(line -> mounters.append("      ").append(line).append('\n'));
        StringBuilder yaml = new StringBuilder("infrastructure:\n  bus: {kind: EVENT_BUS, type: IN_MEMORY}\n");
        if (withDataLake) yaml.append(piece("lake", "DATA_LAKE", dataLake));
        if (withDataMart) yaml.append(piece("mart", "DATA_MART", dataMart));
        dataMarts.forEach((name, path) -> yaml.append(piece(name, "DATA_MART", path)));
        if (withQuarantine && quarantine != null) yaml.append(piece("quarantine", "QUARANTINE_REPOSITORY", quarantine));
        aggregateMetadata.ifPresent(path -> yaml.append(piece("aggregates", "AGGREGATE_METADATA", path)));
        descriptor.write(yaml.append("modules:\n").append(mounters).toString());
    }

    private static String piece(String name, String kind, Path path) {
        return "  " + name + ": {kind: " + kind + ", type: FILE_SYSTEM, configuration: {path: \""
                + path.toString().replace('\\', '/') + "\"}}\n";
    }

    private static Envelope envelope(String source, Map<String, String> record) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", record.get("id"));
        fields.put("amount", Long.parseLong(record.get("amount")));
        Optional.ofNullable(record.get("country")).filter(country -> !country.isBlank())
                .ifPresent(country -> fields.put("country", country));
        return new Envelope(IdempotencyKey.of(record.get("id")), new EventMetadata().set(EventMetadataKey.SOURCE, source),
                MediaType.of("application/json"), RecordSerializerFactory.record().serialize(fields), EnvelopeStatus.CAPTURED, 0,
                Instant.now(), Instant.now());
    }

    private static GenericRecord silverRecord(Envelope envelope) {
        Map<String, Object> fields = new LinkedHashMap<>(PayloadDeserializerFactory.json().asMap()
                .deserialize(RecordSerializerFactory.record().serialize(envelope)));
        fields.put("payload", PayloadDeserializerFactory.json().asMap().deserialize(envelope.payload()));
        return GenericRecord.of(fields);
    }

    private static PartitionKey partitionOf(String path) {
        return new PartitionKey(KeyEntries.join(List.of(path.split("/"))));
    }

    private Path silver() {
        return dataLake.resolve("silver");
    }

    private boolean goldWasWritten() {
        return !filesUnder(gold()).isEmpty()
                || dataMarts.values().stream().anyMatch(dataMart -> !filesUnder(dataMart.resolve("gold")).isEmpty());
    }

    private Path gold() {
        return dataMart.resolve("gold");
    }

    private static String contentUnder(Path root) {
        return String.join("\n", filesUnder(root).stream().map(MounterSteps::read).toList());
    }

    private static List<Path> filesUnder(Path root) {
        if (!Files.exists(root)) return List.of();
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void eventually(Runnable assertion) {
        Instant deadline = Instant.now().plus(MOUNT_TIMEOUT);
        while (true) {
            try {
                assertion.run();
                return;
            } catch (AssertionError e) {
                if (Instant.now().isAfter(deadline)) throw e;
                sleep();
            }
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
