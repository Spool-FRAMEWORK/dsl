package software.spool.dsl.source;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public final class FileSystemDescriptorSource implements DescriptorSource {

    /** The location of a file as this source names it, so the same file always gets the same location. */
    public static String locationOf(Path file) {
        return file.toAbsolutePath().normalize().toString();
    }

    @Override
    public String name() {
        return "the file system";
    }

    @Override
    public Optional<byte[]> read(String location) throws IOException {
        Path file = Path.of(location);
        if (!Files.isRegularFile(file)) return Optional.empty();
        return Optional.of(Files.readAllBytes(file));
    }

    @Override
    public Optional<List<String>> list(String folder) throws IOException {
        Path root = Path.of(folder);
        if (!Files.isDirectory(root)) return Optional.empty();
        try (Stream<Path> files = Files.walk(root)) {
            return Optional.of(files.filter(Files::isRegularFile).map(FileSystemDescriptorSource::locationOf).toList());
        }
    }

    @Override
    public String resolve(String from, String relative) {
        return locationOf(Path.of(from).resolveSibling(relative));
    }
}
