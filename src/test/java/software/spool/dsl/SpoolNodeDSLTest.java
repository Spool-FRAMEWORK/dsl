package software.spool.dsl;

import org.junit.jupiter.api.Test;
import software.spool.core.model.spool.SpoolNode;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpoolNodeDSLTest {

    @Test
    void fromDescriptor_nonExistentResource_throwsIOException() {
        assertThatThrownBy(() -> SpoolNodeDSL.fromDescriptor("/non-existent.yaml"))
            .isInstanceOf(IOException.class);
    }

    @Test
    void fromDescriptor_emptyModules_returnsNode() {
        InfrastructureComponentDescriptor component = new InfrastructureComponentDescriptor("in-memory", Map.of());
        InfrastructureDescriptor infra = new InfrastructureDescriptor(null, component, component, component);
        SpoolNodeDescriptor descriptor = new SpoolNodeDescriptor(infra, List.of());

        SpoolNode node = SpoolNodeDSL.fromDescriptor(descriptor);

        assertThat(node).isNotNull();
    }
}
