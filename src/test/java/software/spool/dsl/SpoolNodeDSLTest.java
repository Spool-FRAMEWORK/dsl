package software.spool.dsl;

import org.junit.jupiter.api.Test;
import software.spool.core.model.spool.SpoolNode;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpoolNodeDSLTest {

    @Test
    void fromDescriptor_nonExistentResource_throwsIOException() {
        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor("/non-existent.yaml"))
            .isInstanceOf(IOException.class)
            .hasMessage("Descriptor not found in the classpath: /non-existent.yaml");
    }

    @Test
    void fromDescriptor_descriptorWithAMissingKey_reportsTheFileAndTheKey() {
        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor("/descriptors/crawler-without-source.yaml"))
            .isInstanceOf(IOException.class)
            .hasMessage("Invalid descriptor /descriptors/crawler-without-source.yaml: "
                + "modules[0].crawler.source is required")
            .hasCauseInstanceOf(InvalidDescriptorException.class);
    }

    @Test
    void fromDescriptor_fileThatIsNotYaml_reportsTheFileAndKeepsTheCause() {
        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor("/descriptors/not-yaml.yaml"))
            .isInstanceOf(IOException.class)
            .hasMessageStartingWith("Could not load descriptor /descriptors/not-yaml.yaml: ")
            .hasCauseInstanceOf(Exception.class);
    }

    @Test
    void fromDescriptor_crawlerWhoseSourceIsASingleJsonObject_buildsTheNode() throws IOException {
        SpoolNode node = SpoolNodeDSL.fromDescriptor("/descriptors/crawler-json-object.yaml");

        assertThat(node).isNotNull();
    }

    @Test
    void fromDescriptor_emptyModules_returnsNode() {
        SpoolNodeDescriptor descriptor = Pieces.node(List.of(), Pieces.eventBus("in-memory"));

        SpoolNode node = SpoolNodeDSL.fromDescriptor(descriptor);

        assertThat(node).isNotNull();
    }

    @Test
    void fromDescriptor_ingesterWithoutDataLake_saysWhichModuleNeedsIt() {
        SpoolNodeDescriptor descriptor = Pieces.node(List.of(IngesterDescriptor.withDefaults(IngesterType.REACTIVE, "synthea-ingester")),
            Pieces.eventBus("IN_MEMORY"), Pieces.inbox("IN_MEMORY", Map.of()));

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(descriptor))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessage("module 'synthea-ingester': the module needs a DATA_LAKE, and infrastructure declares none");
    }

    @Test
    void fromDescriptor_ingesterWithoutQuarantine_buildsTheNode() {
        SpoolNodeDescriptor descriptor = Pieces.node(List.of(IngesterDescriptor.withDefaults(IngesterType.REACTIVE, "synthea-ingester")),
            Pieces.eventBus("IN_MEMORY"), Pieces.inbox("FILE_SYSTEM", Map.of("path", "target/spool-test/inbox")),
            Pieces.dataLake("FILE_SYSTEM", Map.of("path", "target/spool-test/datalake")));

        SpoolNode node = SpoolNodeDSL.fromDescriptor(descriptor);

        assertThat(node).isNotNull();
    }

    @Test
    void fromDescriptor_janitorWithoutQuarantineRepository_saysWhichModuleNeedsIt() {
        SpoolNodeDescriptor descriptor = Pieces.node(List.of(JanitorDescriptor.withDefaults("synthea-janitor")),
            Pieces.eventBus("IN_MEMORY"), Pieces.inbox("FILE_SYSTEM", Map.of("path", "target/spool-test/inbox")));

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(descriptor))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessage("module 'synthea-janitor': the module needs a QUARANTINE_REPOSITORY, and infrastructure declares none");
    }

    @Test
    void fromDescriptor_s3DataLakeWithHalfTheCredentials_saysWhichKeyIsMissing() {
        SpoolNodeDescriptor descriptor = Pieces.node(List.of(IngesterDescriptor.withDefaults(IngesterType.REACTIVE, "synthea-ingester")),
            Pieces.eventBus("IN_MEMORY"), Pieces.inbox("IN_MEMORY", Map.of()), Pieces.dataLake("S3", Map.of(
                "region", "auto", "bucket", "spool", "endpoint", "http://localhost:9000", "accessKeyEnv", "R2_ACCESS_KEY")));

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(descriptor))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("S3 credentials need both accessKeyEnv and secretKeyEnv, but secretKeyEnv is missing");
    }
}
