package software.spool.dsl;

import software.spool.dsl.compose.DescriptorComposer;
import software.spool.dsl.source.DescriptorSource;
import software.spool.dsl.yaml.YamlTreeParser;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The files a descriptor is made of: itself and every file it includes, and the ones those include, in the order they
 * are read. An include of a file that does not exist fails as it does when a node loads it.
 */
public final class DescriptorFiles {
    private DescriptorFiles() {
    }

    /** @throws IOException when the descriptor or one of its includes cannot be read */
    public static List<String> of(DescriptorSource source, String root) throws IOException {
        Recording recording = new Recording(source);
        try {
            new DescriptorComposer(recording, new YamlTreeParser()).compose(root, Optional.empty());
        } catch (InvalidDescriptorException e) {
            throw new IOException("Invalid descriptor " + root + ": " + e.getMessage(), e);
        }
        return List.copyOf(recording.read);
    }

    private static final class Recording implements DescriptorSource {
        private final DescriptorSource source;
        private final Set<String> read = new LinkedHashSet<>();

        Recording(DescriptorSource source) {
            this.source = source;
        }

        @Override
        public String name() {
            return source.name();
        }

        @Override
        public Optional<byte[]> read(String location) throws IOException {
            Optional<byte[]> content = source.read(location);
            content.ifPresent(found -> read.add(location));
            return content;
        }

        @Override
        public Optional<List<String>> list(String folder) throws IOException {
            return source.list(folder);
        }

        @Override
        public String resolve(String from, String relative) {
            return source.resolve(from, relative);
        }
    }
}
