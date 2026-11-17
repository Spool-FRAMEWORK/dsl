package software.spool.dsl.acceptance;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.dsl.DescriptorLoader;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class FileSystemDescriptorSteps {
    private final DescriptorLoader loader = DescriptorLoader.fromFileSystem(name -> null);
    private Path folder;
    private Optional<SpoolNodeDescriptor> descriptor = Optional.empty();
    private Optional<IOException> failure = Optional.empty();

    @Before
    public void createFolder() throws IOException {
        folder = Files.createTempDirectory("spool-descriptor");
    }

    @After
    public void deleteFolder() throws IOException {
        try (Stream<Path> files = Files.walk(folder)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(file);
            }
        }
    }

    @Given("the file {string} with an event bus of type {string}")
    public void theFileWithAnEventBus(String name, String type) throws IOException {
        write(name, """
                infrastructure:
                  eventBus:
                    type: %s
                """.formatted(type));
    }

    @Given("the file {string} that includes {string}")
    public void theFileThatIncludes(String name, String included) throws IOException {
        write(name, """
                include:
                  - %s

                modules: []
                """.formatted(included));
    }

    @Given("the file {string} with an event bus of type {string} and a profile {string} that makes it {string}")
    public void theFileWithAProfile(String name, String type, String profile, String profileType) throws IOException {
        write(name, """
                infrastructure:
                  eventBus:
                    type: %s

                modules: []

                profiles:
                  %s:
                    infrastructure:
                      eventBus:
                        type: %s
                """.formatted(type, profile, profileType));
    }

    @When("{string} is loaded from the file system")
    public void isLoaded(String name) {
        load(() -> loader.load(pathOf(name)));
    }

    @When("{string} is loaded from the file system with the profile {string}")
    public void isLoadedWithTheProfile(String name, String profile) {
        load(() -> loader.load(pathOf(name), profile));
    }

    @Then("the event bus is of type {string}")
    public void theEventBusIsOfType(String type) {
        assertThat(failure).isEmpty();
        assertThat(descriptor).hasValueSatisfying(
                node -> assertThat(node.infrastructure().eventBus().type()).isEqualTo(type));
    }

    @Then("loading fails with the cycle {string}")
    public void loadingFailsWithTheCycle(String cycle) {
        String withPaths = Arrays.stream(cycle.split(" -> ")).map(this::pathOf).collect(Collectors.joining(" -> "));
        assertThat(failure).hasValueSatisfying(error -> assertThat(error).hasMessageContaining(withPaths));
    }

    private void write(String name, String content) throws IOException {
        Path file = folder.resolve(name);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    private String pathOf(String name) {
        return folder.resolve(name).toAbsolutePath().normalize().toString();
    }

    private void load(Load load) {
        try {
            descriptor = Optional.of(load.run());
        } catch (IOException error) {
            failure = Optional.of(error);
        }
    }

    @FunctionalInterface
    private interface Load {
        SpoolNodeDescriptor run() throws IOException;
    }
}
