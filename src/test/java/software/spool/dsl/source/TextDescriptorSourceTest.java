package software.spool.dsl.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.DescriptorLoader;
import software.spool.dsl.DescriptorModules;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TextDescriptorSourceTest {
    private static final String DESCRIPTOR = """
            infrastructure:
              bus: {kind: EVENT_BUS, type: IN_MEMORY}
              inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "%s"}}
              lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: "%s"}}
            modules:
              - ingester: {type: REACTIVE, id: orders-ingester}
            """;

    @TempDir
    Path folder;

    @Test
    void aDescriptorGivenAsText_buildsItsModulesWithoutANode() throws IOException {
        List<SpoolModule> modules = loader(descriptor()).load(TextDescriptorSource.LOCATION, Optional.empty(),
                DescriptorModules::of);

        assertThat(modules).singleElement()
                .satisfies(module -> assertThat(module.checkHealth().moduleId()).isEqualTo("orders-ingester"));
    }

    @Test
    void anInvalidDescriptorGivenAsText_failsWithTheErrorOfTheDsl() {
        String invalid = descriptor().replace("lake: {kind: DATA_LAKE, ", "lake: {");

        assertThatThrownBy(() -> loader(invalid).load(TextDescriptorSource.LOCATION))
                .isInstanceOf(IOException.class)
                .hasMessageStartingWith("Invalid descriptor " + TextDescriptorSource.LOCATION)
                .hasMessageContaining("infrastructure.lake.kind");
    }

    @Test
    void anIncludeOfADescriptorGivenAsText_isNotFound() {
        String including = "include: [shared.spool.yaml]\n" + descriptor();

        assertThatThrownBy(() -> loader(including).load(TextDescriptorSource.LOCATION))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("shared.spool.yaml");
    }

    private String descriptor() {
        return DESCRIPTOR.formatted(slashed(folder.resolve("inbox")), slashed(folder.resolve("lake")));
    }

    private static DescriptorLoader loader(String text) {
        return new DescriptorLoader(new TextDescriptorSource(text), Map.<String, String>of()::get);
    }

    private static String slashed(Path path) {
        return path.toString().replace('\\', '/');
    }
}
