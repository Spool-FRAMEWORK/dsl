package software.spool.dsl.acceptance;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writes and loads the descriptor of a scenario, whole or put together from the providers the scenario declares,
 * and checks what any feature can check on it.
 */
public class DescriptorSteps {
    private final DescriptorUnderTest descriptor;
    private final Map<String, String> providers = new LinkedHashMap<>();

    public DescriptorSteps(DescriptorUnderTest descriptor) {
        this.descriptor = descriptor;
    }

    @Given("the descriptor:")
    public void theDescriptor(String yaml) {
        descriptor.write(yaml);
    }

    @Given("the environment variable {string} is {string}")
    public void theEnvironmentVariableIs(String name, String value) {
        descriptor.setEnvironmentVariable(name, value);
    }

    @Given("the descriptor declares the provider {string} of type {string}")
    public void theDescriptorDeclaresTheProvider(String name, String type) {
        declare(name, "{type: " + type + ", configuration: " + configurationFor(type) + "}");
    }

    @Given("the descriptor declares the provider {string} of type {string} without an issuer")
    public void theDescriptorDeclaresTheProviderWithoutAnIssuer(String name, String type) {
        declare(name, "{type: " + type + ", configuration: {audience: spool-janitor}}");
    }

    @When("the descriptor is loaded")
    public void theDescriptorIsLoaded() {
        descriptor.load(Optional.empty());
    }

    @When("the descriptor is loaded with the profile {string}")
    public void theDescriptorIsLoadedWithTheProfile(String profile) {
        descriptor.load(Optional.of(profile));
    }

    @Then("the inbox path is {string}")
    public void theInboxPathIs(String path) {
        assertThat(descriptor.loaded().infrastructure().inbox().configuration()).containsEntry("path", path);
    }

    @Then("the data lake path is {string}")
    public void theDataLakePathIs(String path) {
        assertThat(descriptor.loaded().infrastructure().dataLake().configuration()).containsEntry("path", path);
    }

    @Then("the node knows the providers {string} and {string}")
    public void theNodeKnowsTheProviders(String first, String second) {
        assertThat(descriptor.loaded().authentication()).containsOnlyKeys(first, second);
    }

    @Then("the load fails")
    public void theLoadFails() {
        descriptor.failure();
    }

    @Then("the error names {string}")
    public void theErrorNames(String text) {
        assertThat(descriptor.failure().getMessage()).contains(text);
    }

    @Then("the error does not contain {string}")
    public void theErrorDoesNotContain(String text) {
        assertThat(descriptor.failure().getMessage()).doesNotContain(text);
    }

    private void declare(String name, String provider) {
        providers.put(name, provider);
        StringBuilder yaml = new StringBuilder("infrastructure:\n  eventBus: {type: IN_MEMORY}\nauthentication:\n");
        providers.forEach((declared, body) -> yaml.append("  ").append(declared).append(": ").append(body).append('\n'));
        descriptor.write(yaml.append("modules: []\n").toString());
    }

    private static String configurationFor(String type) {
        return switch (type) {
            case "OIDC" -> "{issuer: 'https://identity.example/realms/spool', audience: spool-janitor}";
            case "HMAC_SIGNATURE" -> "{header: X-Signature-256, secret: webhook-secret}";
            default -> "{}";
        };
    }
}
