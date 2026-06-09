package software.spool.dsl.yaml;

import org.junit.jupiter.api.Test;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.yaml.raw.RawComponentDescriptor;
import software.spool.dsl.yaml.raw.RawInfrastructureDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DescriptorMapperTest {

    @Test
    void map_ingesterModule_returnsIngesterDescriptor() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(
            anyInfra(),
            List.of(Map.of("ingester", Map.of("type", "polling", "id", "test-ingester")))
        );

        SpoolNodeDescriptor result = DescriptorMapper.map(raw);

        assertThat(result.modules()).hasSize(1);
        assertThat(result.modules().get(0)).isInstanceOf(IngesterDescriptor.class);
        assertThat(((IngesterDescriptor) result.modules().get(0)).id()).isEqualTo("test-ingester");
    }

    @Test
    void map_unknownModuleType_throwsIllegalArgumentException() {
        RawSpoolNodeDescriptor raw = new RawSpoolNodeDescriptor(
            anyInfra(),
            List.of(Map.of("unknown", Map.of("id", "x")))
        );

        assertThatThrownBy(() -> DescriptorMapper.map(raw))
            .isInstanceOf(IllegalArgumentException.class);
    }

    private static RawInfrastructureDescriptor anyInfra() {
        RawComponentDescriptor component = new RawComponentDescriptor("in-memory", null);
        return new RawInfrastructureDescriptor(null, component, component, component);
    }
}
