package software.spool.dsl.acceptance.folder;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import software.spool.dsl.DescriptorFolder;
import software.spool.dsl.NamedDescriptor;
import software.spool.dsl.acceptance.DescriptorUnderTest;
import software.spool.dsl.source.ClasspathDescriptorSource;
import software.spool.dsl.source.DescriptorSource;
import software.spool.dsl.source.FileSystemDescriptorSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class DescriptorFolderSteps {
    private final DescriptorUnderTest descriptor;
    private final Path folder;
    private List<NamedDescriptor> loaded = List.of();
    private Optional<IOException> failure = Optional.empty();

    public DescriptorFolderSteps(DescriptorUnderTest descriptor) throws IOException {
        this.descriptor = descriptor;
        this.folder = Files.createTempDirectory("spool-descriptor-folder");
    }

    @Given("the folder has the file {string}:")
    public void theFolderHasTheFile(String name, String content) throws IOException {
        Path file = folder.resolve(name);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    @When("the folder is loaded from the file system")
    public void theFolderIsLoadedFromTheFileSystem() {
        load(new FileSystemDescriptorSource(), FileSystemDescriptorSource.locationOf(folder));
    }

    @When("the folder {string} is loaded from the classpath")
    public void theFolderIsLoadedFromTheClasspath(String resourceFolder) {
        load(new ClasspathDescriptorSource(), resourceFolder);
    }

    @Then("the node opens its HTTP server on the port {int}")
    public void theNodeOpensItsHttpServerOnThePort(int port) {
        assertThat(descriptor.loaded().server().port()).isEqualTo(port);
    }

    @Then("the folder gives the nodes {string} and {string}")
    public void theFolderGivesTheNodes(String first, String second) {
        assertThat(failure).isEmpty();
        assertThat(loaded).extracting(NamedDescriptor::name).containsExactly(first, second);
    }

    @Then("the node of {string} opens its HTTP server on the port {int}")
    public void theNodeOfOpensItsHttpServerOnThePort(String name, int port) {
        assertThat(loaded).filteredOn(named -> named.name().equals(name)).singleElement()
                .satisfies(named -> assertThat(named.descriptor().server().port()).isEqualTo(port));
    }

    @Then("loading the folder fails")
    public void loadingTheFolderFails() {
        assertThat(failure).as("the folder was expected not to load").isPresent();
    }

    @Then("the error of the folder names {string}")
    public void theErrorOfTheFolderNames(String text) {
        assertThat(failure).hasValueSatisfying(error -> assertThat(error.getMessage()).contains(text));
    }

    private void load(DescriptorSource source, String location) {
        try {
            loaded = new DescriptorFolder(source, name -> null).load(location, Optional.empty());
        } catch (IOException e) {
            failure = Optional.of(e);
        }
    }
}
