package software.spool.dsl;

import org.junit.jupiter.api.Test;
import software.spool.core.model.spool.SpoolNode;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;

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
        InfrastructureComponentDescriptor component = new InfrastructureComponentDescriptor("in-memory", Map.of());
        InfrastructureDescriptor infra = new InfrastructureDescriptor(null, component, component, component);
        SpoolNodeDescriptor descriptor = new SpoolNodeDescriptor(infra, List.of());

        SpoolNode node = SpoolNodeDSL.fromDescriptor(descriptor);

        assertThat(node).isNotNull();
    }

    @Test
    void fromDescriptor_ingesterWithoutDataLake_saysWhichModuleNeedsIt() {
        InfrastructureComponentDescriptor bus = new InfrastructureComponentDescriptor("IN_MEMORY", Map.of());
        InfrastructureDescriptor infra = new InfrastructureDescriptor(null, bus, bus, null);
        SpoolNodeDescriptor descriptor = new SpoolNodeDescriptor(infra,
            List.of(new IngesterDescriptor("REACTIVE", "synthea-ingester", Map.of())));

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(descriptor))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessage("module 'synthea-ingester': infrastructure.dataLake is required");
    }

    @Test
    void fromDescriptor_s3DataLakeWithHalfTheCredentials_saysWhichKeyIsMissing() {
        InfrastructureComponentDescriptor bus = new InfrastructureComponentDescriptor("IN_MEMORY", Map.of());
        InfrastructureComponentDescriptor lake = new InfrastructureComponentDescriptor("S3", Map.of(
            "region", "auto", "bucket", "spool", "endpoint", "http://localhost:9000", "accessKeyEnv", "R2_ACCESS_KEY"));
        InfrastructureDescriptor infra = new InfrastructureDescriptor(null, bus, bus, lake);
        SpoolNodeDescriptor descriptor = new SpoolNodeDescriptor(infra,
            List.of(new IngesterDescriptor("REACTIVE", "synthea-ingester", Map.of())));

        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor(descriptor))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("S3 credentials need both accessKeyEnv and secretKeyEnv, but secretKeyEnv is missing");
    }
}
