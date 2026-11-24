package software.spool.dsl.source;

import org.junit.jupiter.api.Test;
import software.spool.dsl.DescriptorFiles;
import software.spool.dsl.DescriptorLoader;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryDescriptorSourceTest {
    private static final Map<String, String> PIPELINE = Map.of(
            "orders.spool.yaml", "include: [shared/infrastructure.yaml]\nmodules: []\n",
            "shared/infrastructure.yaml", "include: [bus.yaml]\n",
            "shared/bus.yaml", "infrastructure:\n  bus: {kind: EVENT_BUS, type: IN_MEMORY}\n");

    @Test
    void aDescriptorSplitInFiles_isLoadedWithItsIncludesRelativeToTheFileThatNamesThem() throws IOException {
        SpoolNodeDescriptor loaded = new DescriptorLoader(new InMemoryDescriptorSource(PIPELINE), Map.<String, String>of()::get)
                .load("orders.spool.yaml");

        assertThat(loaded.infrastructure().pieces()).extracting(piece -> piece.name()).containsExactly("bus");
    }

    @Test
    void theFilesOfADescriptor_areItAndEveryFileItIncludes_inTheOrderTheyAreRead() throws IOException {
        assertThat(DescriptorFiles.of(new InMemoryDescriptorSource(PIPELINE), "orders.spool.yaml"))
                .containsExactly("orders.spool.yaml", "shared/infrastructure.yaml", "shared/bus.yaml");
    }

    @Test
    void anIncludeOfAFileThatIsNotThere_failsNamingIt() {
        Map<String, String> withoutBus = Map.of(
                "orders.spool.yaml", PIPELINE.get("orders.spool.yaml"),
                "shared/infrastructure.yaml", PIPELINE.get("shared/infrastructure.yaml"));

        assertThatThrownBy(() -> DescriptorFiles.of(new InMemoryDescriptorSource(withoutBus), "orders.spool.yaml"))
                .isInstanceOf(IOException.class).hasMessageContaining("shared/bus.yaml");
    }
}
