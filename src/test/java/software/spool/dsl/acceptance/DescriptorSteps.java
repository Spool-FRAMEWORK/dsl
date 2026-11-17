package software.spool.dsl.acceptance;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.dsl.DescriptorLoader;
import software.spool.dsl.SpoolNodeDSL;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.source.DescriptorSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Loads a descriptor the way a node does, building the node from it, so that every check the load makes runs. The
 * descriptor is written whole, or put together from the providers a scenario declares.
 */
public class DescriptorSteps {
    private static final String LOCATION = "/Spool.yaml";

    private final Map<String, String> environment = new HashMap<>();
    private final Map<String, String> providers = new LinkedHashMap<>();
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

    @Given("the descriptor declares the provider {string} of type {string}")
    public void theDescriptorDeclaresTheProvider(String name, String type) {
        providers.put(name, "{type: " + type + ", configuration: " + configurationFor(type) + "}");
    }

    @Given("the descriptor declares the provider {string} of type {string} without an issuer")
    public void theDescriptorDeclaresTheProviderWithoutAnIssuer(String name, String type) {
        providers.put(name, "{type: " + type + ", configuration: {audience: spool-janitor}}");
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

    @Then("the node knows the providers {string} and {string}")
    public void theNodeKnowsTheProviders(String first, String second) {
        assertThat(failure).isNull();
        assertThat(loaded.authentication()).containsOnlyKeys(first, second);
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
        String content = descriptor != null ? descriptor : descriptorWithTheProviders();
        DescriptorLoader loader = new DescriptorLoader(new InMemorySource(Map.of(LOCATION, content)), environment::get);
        try {
            loaded = loader.load(LOCATION, profile, node -> {
                SpoolNodeDSL.fromDescriptor(node);
                return node;
            });
        } catch (IOException e) {
            failure = e;
        }
    }

    private String descriptorWithTheProviders() {
        StringBuilder yaml = new StringBuilder("infrastructure:\n  eventBus: {type: IN_MEMORY}\nauthentication:\n");
        providers.forEach((name, provider) -> yaml.append("  ").append(name).append(": ").append(provider).append('\n'));
        return yaml.append("modules: []\n").toString();
    }

    private static String configurationFor(String type) {
        return switch (type) {
            case "OIDC" -> "{issuer: 'https://identity.example/realms/spool', audience: spool-janitor}";
            case "HMAC_SIGNATURE" -> "{header: X-Signature-256, secret: webhook-secret}";
            default -> "{}";
        };
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
