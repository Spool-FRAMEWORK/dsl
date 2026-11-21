package software.spool.dsl.acceptance;

import software.spool.dsl.DescriptorLoader;
import software.spool.dsl.SpoolNodeDSL;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.source.DescriptorSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The descriptor a scenario writes, the environment it sees and what loading it gave. It is loaded the way a node
 * does, building the node, so every check of the load runs. Cucumber hands the same instance to every step class of
 * a scenario, so their steps can be combined freely.
 */
public class DescriptorUnderTest {
    private static final String LOCATION = "/Spool.yaml";

    private final Map<String, String> environment = new HashMap<>();
    private String yaml = "";
    private Optional<SpoolNodeDescriptor> loaded = Optional.empty();
    private Optional<IOException> failure = Optional.empty();

    public void write(String yaml) {
        this.yaml = yaml;
    }

    public void setEnvironmentVariable(String name, String value) {
        environment.put(name, value);
    }

    public void load(Optional<String> profile) {
        DescriptorLoader loader = new DescriptorLoader(new InMemorySource(Map.of(LOCATION, yaml)), environment::get);
        try {
            loaded = Optional.of(loader.load(LOCATION, profile, node -> {
                SpoolNodeDSL.fromDescriptor(node);
                return node;
            }));
        } catch (IOException e) {
            failure = Optional.of(e);
        }
    }

    public boolean hasLoaded() {
        return loaded.isPresent();
    }

    /** What loading gave, failing the scenario when it did not load. */
    public SpoolNodeDescriptor loaded() {
        assertThat(failure).as(() -> "the descriptor was expected to load, but: "
                + failure.map(IOException::getMessage).orElse("")).isEmpty();
        return loaded.orElseThrow();
    }

    /** Why loading failed, failing the scenario when it loaded. */
    public IOException failure() {
        assertThat(failure).as("the descriptor was expected not to load").isPresent();
        return failure.orElseThrow();
    }

    private record InMemorySource(Map<String, String> files) implements DescriptorSource {
        @Override
        public String name() {
            return "memory";
        }

        @Override
        public Optional<byte[]> read(String location) {
            return Optional.ofNullable(files.get(location)).map(content -> content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public Optional<List<String>> list(String folder) {
            return Optional.empty();
        }

        @Override
        public String resolve(String from, String relative) {
            return relative;
        }
    }
}
