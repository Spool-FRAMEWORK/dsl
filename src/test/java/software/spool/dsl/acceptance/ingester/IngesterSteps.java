package software.spool.dsl.acceptance.ingester;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class IngesterSteps {
    private static final String INGESTER = "orders-ingester";
    private static final String DESCRIPTOR = """
            infrastructure:
              bus: {kind: EVENT_BUS, type: IN_MEMORY}
              inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "%s"}}
              lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: "%s"}}
            modules:
              - ingester: {%sid: %s}
            """;

    private final DescriptorUnderTest descriptor;

    public IngesterSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("a descriptor with an ingester of type {string}")
    public void anIngesterOfType(String type) throws IOException {
        write("type: " + type + ", ");
    }

    @Given("a descriptor with an ingester without a type")
    public void anIngesterWithoutAType() throws IOException {
        write("");
    }

    @When("the node is built from it")
    public void theNodeIsBuilt() {
        descriptor.load(Optional.empty());
    }

    @Then("the node has the ingester")
    public void theNodeHasTheIngester() {
        assertThat(descriptor.loaded().modules()).extracting(SpoolModuleDescriptor::id).containsExactly(INGESTER);
    }

    @Then("building fails with an error that says {string}")
    public void buildingFails(String text) {
        assertThat(descriptor.failure()).hasMessageContaining(text);
    }

    private void write(String type) throws IOException {
        Path folder = Files.createTempDirectory("spool-ingester");
        descriptor.write(DESCRIPTOR.formatted(slashed(folder.resolve("inbox")), slashed(folder.resolve("lake")), type, INGESTER));
    }

    private static String slashed(Path path) {
        return path.toString().replace('\\', '/');
    }
}
