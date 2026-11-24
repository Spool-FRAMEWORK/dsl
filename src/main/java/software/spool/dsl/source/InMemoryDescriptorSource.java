package software.spool.dsl.source;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Descriptors that arrive as texts, each at a path such as {@code shared/infrastructure.yaml}, like the files of a
 * folder. An {@code include:} names a file by its path relative to the file that includes it, as in a folder.
 */
public final class InMemoryDescriptorSource implements DescriptorSource {
    private final Map<String, String> files;

    public InMemoryDescriptorSource(Map<String, String> files) {
        this.files = Map.copyOf(files);
    }

    @Override
    public String name() {
        return "the files sent";
    }

    @Override
    public Optional<byte[]> read(String location) {
        return Optional.ofNullable(files.get(location)).map(text -> text.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Optional<List<String>> list(String folder) {
        String prefix = folder.isEmpty() || folder.endsWith("/") ? folder : folder + "/";
        List<String> inside = files.keySet().stream().filter(path -> path.startsWith(prefix)).sorted().toList();
        return inside.isEmpty() ? Optional.empty() : Optional.of(inside);
    }

    @Override
    public String resolve(String from, String relative) {
        return URI.create(from).resolve(relative).normalize().toString();
    }
}
