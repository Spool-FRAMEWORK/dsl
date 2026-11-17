package software.spool.dsl.acceptance.variables;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.dsl.DescriptorLoader;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.source.DescriptorSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class DescriptorVariablesSteps {
    private static final String LOCATION = "/Spool.yaml";

    private final Map<String, String> environment = new HashMap<>();
    private String descriptor;
    private SpoolNodeDescriptor loaded;
    private IOException failure;

    @Given("the descriptor:")
    public void theDescriptor(String yaml) {
        descriptor = yaml;
    }

    @Given("the environment variable {string} is {string}")
    public void theEnvironmentVariableIs(String name, String value) {
        environment.put(name, value);
    }

    @When("the descriptor is loaded")
    public void theDescriptorIsLoaded() {
        load(Optional.empty());
    }

    @When("the descriptor is loaded with the profile {string}")
    public void theDescriptorIsLoadedWithTheProfile(String profile) {
        load(Optional.of(profile));
    }

    @Then("the inbox path is {string}")
    public void theInboxPathIs(String path) {
        assertThat(failure).isNull();
        assertThat(loaded.infrastructure().inbox().configuration()).containsEntry("path", path);
    }

    @Then("the data lake path is {string}")
    public void theDataLakePathIs(String path) {
        assertThat(failure).isNull();
        assertThat(loaded.infrastructure().dataLake().configuration()).containsEntry("path", path);
    }

    @Then("the load fails")
    public void theLoadFails() {
        assertThat(failure).as("the descriptor was expected not to load").isNotNull();
    }

    @Then("the error names {string}")
    public void theErrorNames(String text) {
        assertThat(failure.getMessage()).contains(text);
    }

    @Then("the error does not contain {string}")
    public void theErrorDoesNotContain(String text) {
        assertThat(failure.getMessage()).doesNotContain(text);
    }

    private void load(Optional<String> profile) {
        DescriptorLoader loader = new DescriptorLoader(new InMemorySource(Map.of(LOCATION, descriptor)), environment::get);
        try {
            loaded = loader.load(LOCATION, profile, node -> node);
        } catch (IOException e) {
            failure = e;
        }
    }

    private record InMemorySource(Map<String, String> files) implements DescriptorSource {
        @Override
        public String name() {
            return "memory";
        }

        @Override
        public Optional<byte[]> read(String location) {
            return Optional.ofNullable(files.get(location)).map(content -> content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String resolve(String from, String relative) {
            return relative;
        }
    }
}
