package software.spool.dsl.acceptance;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class PieceHealthSteps {
    private static final String HEALTH_PATH = "/spool/health";
    private static final String UNREADABLE_RECORD = "unreadable-record.json";
    private static final Duration WAIT_FOR_A_MODULE = Duration.ofSeconds(10);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

    private final MounterSteps mounter;
    private HttpResponse<String> health;

    public PieceHealthSteps(MounterSteps mounter) {
        this.mounter = mounter;
    }

    @Given("the folder of the data lake cannot be written")
    public void theFolderOfTheDataLakeCannotBeWritten() throws IOException {
        Path folder = mounter.dataLakeFolder();
        try (Stream<Path> paths = Files.walk(folder)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
        }
        Files.createFile(folder);
    }

    @When("the health of the node is asked")
    public void theHealthOfTheNodeIsAsked() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + mounter.nodePort() + HEALTH_PATH)).GET().build();
        try (HttpClient client = HttpClient.newHttpClient()) {
            health = client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    @Given("bronze has a record in the partition {string} that cannot be read")
    public void bronzeHasARecordThatCannotBeRead(String partition) throws IOException {
        Path folder = mounter.dataLakeFolder().resolve("bronze").resolve(partition);
        Files.createDirectories(folder);
        Files.writeString(folder.resolve(UNREADABLE_RECORD), "this is not a record");
    }

    @Then("the module {string} reports the piece {string} of type {string} as {word}")
    public void theModuleReportsThePiece(String module, String piece, String type, String status) throws Exception {
        long deadline = System.nanoTime() + WAIT_FOR_A_MODULE.toNanos();
        while (!status.equals(pieceOf(module, piece).get("status")) && System.nanoTime() < deadline) {
            Thread.sleep(POLL_INTERVAL.toMillis());
            theHealthOfTheNodeIsAsked();
        }
        assertThat(pieceOf(module, piece)).containsEntry("type", type).containsEntry("status", status);
    }

    @Then("the reason of the piece {string} names the folder of the data lake")
    public void theReasonOfThePieceNamesTheFolderOfTheDataLake(String piece) {
        assertThat(reasonOf(piece)).contains(mounter.dataLakeFolder().toString());
    }

    @Then("the reason of the piece {string} names the record that cannot be read")
    public void theReasonOfThePieceNamesTheRecordThatCannotBeRead(String piece) {
        assertThat(reasonOf(piece)).contains(UNREADABLE_RECORD);
    }

    private String reasonOf(String piece) {
        return modules().stream()
                .flatMap(module -> piecesOf(module).stream())
                .filter(candidate -> piece.equals(candidate.get("piece")))
                .findFirst()
                .map(found -> String.valueOf(found.get("reason")))
                .orElseThrow(() -> new AssertionError("no module reports the piece " + piece + ": " + health.body()));
    }

    @Then("the node is HEALTHY")
    public void theNodeIsHealthy() {
        assertThat(body()).containsEntry("status", "HEALTHY");
        assertThat(health.statusCode()).isEqualTo(200);
    }

    @Then("the node is UNHEALTHY and answers with the status {int}")
    public void theNodeIsUnhealthyAndAnswersWith(int status) {
        assertThat(body()).containsEntry("status", "UNHEALTHY");
        assertThat(health.statusCode()).isEqualTo(status);
    }

    @Then("the piece {string} of the module {string} has no check of its own")
    public void thePieceHasNoCheckOfItsOwn(String piece, String module) {
        assertThat(pieceOf(module, piece)).containsEntry("hasCheck", false);
    }

    private Map<String, Object> pieceOf(String module, String piece) {
        return piecesOf(moduleNamed(module)).stream()
                .filter(candidate -> piece.equals(candidate.get("piece")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the module " + module + " does not report the piece " + piece + ": " + health.body()));
    }

    private Map<String, Object> moduleNamed(String module) {
        return modules().stream()
                .filter(candidate -> module.equals(candidate.get("moduleId")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the node does not report the module " + module + ": " + health.body()));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> modules() {
        return (List<Map<String, Object>>) body().get("modules");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> piecesOf(Map<String, Object> module) {
        return (List<Map<String, Object>>) module.getOrDefault("pieces", List.of());
    }

    private Map<String, Object> body() {
        return PayloadDeserializerFactory.json().asMap().deserialize(health.body().getBytes());
    }
}
