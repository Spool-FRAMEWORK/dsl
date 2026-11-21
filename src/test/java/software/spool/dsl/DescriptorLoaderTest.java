package software.spool.dsl;

import org.junit.jupiter.api.Test;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DescriptorLoaderTest {
    private static final String PROFILES = "/descriptors/compose/profiles.yaml";

    private final DescriptorLoader loader = DescriptorLoader.fromClasspath(name -> null);

    @Test
    void load_withoutProfile_ignoresTheProfiles() throws IOException {
        SpoolNodeDescriptor node = loader.load(PROFILES);

        assertThat(node.infrastructure().piece("eventBus").orElseThrow().type()).isEqualTo("IN_MEMORY");
        assertThat(node.modules()).hasSize(2);
    }

    @Test
    void load_withProfile_overridesOnlyTheKeysItDeclares() throws IOException {
        SpoolNodeDescriptor node = loader.load(PROFILES, "production");

        assertThat(node.infrastructure().piece("eventBus").orElseThrow().type()).isEqualTo("KAFKA");
        assertThat(node.infrastructure().piece("eventBus").orElseThrow().configuration())
                .containsEntry("bootstrapServers", "localhost:9092");
        assertThat(node.infrastructure().piece("inbox").orElseThrow().type()).isEqualTo("FILE_SYSTEM");
        assertThat(node.infrastructure().piece("inbox").orElseThrow().configuration())
                .containsEntry("path", "target/spool-test/inbox");
        assertThat(node.modules()).hasSize(2);
    }

    @Test
    void load_withProfileThatDeclaresAList_replacesTheWholeList() throws IOException {
        SpoolNodeDescriptor node = loader.load(PROFILES, "single");

        assertThat(node.modules()).singleElement()
                .isInstanceOfSatisfying(IngesterDescriptor.class,
                        ingester -> assertThat(ingester.id()).isEqualTo("only-ingester"));
    }

    @Test
    void load_withUnknownProfile_namesTheKnownOnes() {
        assertThatThrownBy(() -> loader.load(PROFILES, "prod"))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid descriptor " + PROFILES
                        + ": profiles does not declare 'prod'. Known profiles: production, single, staging, broken")
                .hasCauseInstanceOf(InvalidDescriptorException.class);
    }

    @Test
    void load_withProfileThatDeclaresAnInclude_fails() {
        String descriptor = "/descriptors/compose/profile-with-include.yaml";

        assertThatThrownBy(() -> loader.load(descriptor, "production"))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid descriptor " + descriptor
                        + ": profiles.production.include is not allowed inside a profile");
    }

    @Test
    void load_withoutProfileArgument_usesTheProfileOfTheEnvironment() throws IOException {
        DescriptorLoader withEnvironment = DescriptorLoader.fromClasspath(
                name -> Map.of(DescriptorLoader.PROFILE_VARIABLE, "production").get(name));

        SpoolNodeDescriptor node = withEnvironment.load(PROFILES);

        assertThat(DescriptorLoader.PROFILE_VARIABLE).isEqualTo("SPOOL_PROFILE");
        assertThat(node.infrastructure().piece("eventBus").orElseThrow().type()).isEqualTo("KAFKA");
    }

    @Test
    void load_withProfileArgument_winsOverTheEnvironment() throws IOException {
        DescriptorLoader withEnvironment = DescriptorLoader.fromClasspath(
                name -> Map.of(DescriptorLoader.PROFILE_VARIABLE, "production").get(name));

        SpoolNodeDescriptor node = withEnvironment.load(PROFILES, "single");

        assertThat(node.infrastructure().piece("eventBus").orElseThrow().type()).isEqualTo("IN_MEMORY");
        assertThat(node.modules()).hasSize(1);
    }

    @Test
    void load_errorInAValueTheProfileSet_saysTheProfile() {
        assertThatThrownBy(() -> loader.load(PROFILES, "broken"))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid descriptor " + PROFILES
                        + ": profile broken: modules[0].crawler.source is required");
    }

    @Test
    void load_unknownKeyInAPiece_failsWithItsPath() {
        String descriptor = "/descriptors/compose/misspelt-key.yaml";

        assertThatThrownBy(() -> loader.load(descriptor))
                .isInstanceOf(IOException.class)
                .hasMessageStartingWith("Invalid descriptor " + descriptor
                        + ": infrastructure.eventBus.tipe is not a known key. Known keys: kind, type, configuration");
    }

    @Test
    void load_withInclude_combinesTheKeysOfTheIncludedFile() throws IOException {
        SpoolNodeDescriptor node = loader.load("/descriptors/compose/with-include.yaml");

        assertThat(node.infrastructure().piece("eventBus").orElseThrow().type()).isEqualTo("IN_MEMORY");
        assertThat(node.infrastructure().piece("inbox").orElseThrow().configuration())
                .containsEntry("path", "target/spool-test/inbox");
        assertThat(node.modules()).singleElement()
                .isInstanceOfSatisfying(IngesterDescriptor.class,
                        ingester -> assertThat(ingester.id()).isEqualTo("own-ingester"));
    }

    @Test
    void load_withInclude_theIncludingFileWins() throws IOException {
        SpoolNodeDescriptor node = loader.load("/descriptors/compose/overrides-include.yaml");

        assertThat(node.infrastructure().piece("eventBus").orElseThrow().type()).isEqualTo("KAFKA");
        assertThat(node.infrastructure().piece("inbox").orElseThrow().type()).isEqualTo("FILE_SYSTEM");
    }

    @Test
    void load_withCircularInclude_namesTheFilesOfTheCycle() {
        assertThatThrownBy(() -> loader.load("/descriptors/compose/cycle-a.yaml"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("include forms a cycle: /descriptors/compose/cycle-a.yaml"
                        + " -> /descriptors/compose/cycle-b.yaml -> /descriptors/compose/cycle-a.yaml");
    }

    @Test
    void load_withMissingInclude_namesItAndTheFileThatAsksForIt() {
        String descriptor = "/descriptors/compose/missing-include.yaml";

        assertThatThrownBy(() -> loader.load(descriptor))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid descriptor " + descriptor
                        + ": include[0] names /descriptors/compose/missing.yaml, which is not in the classpath");
    }

    @Test
    void load_errorInAnIncludedFile_saysTheFileAndThePath() {
        String descriptor = "/descriptors/compose/includes-broken-modules.yaml";

        assertThatThrownBy(() -> loader.load(descriptor))
                .isInstanceOf(IOException.class)
                .hasMessage("Invalid descriptor " + descriptor
                        + ": /descriptors/compose/shared/broken-modules.yaml: modules[0].crawler.source is required")
                .hasCauseInstanceOf(InvalidDescriptorException.class);
    }
}
