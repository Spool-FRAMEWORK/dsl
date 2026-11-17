package software.spool.dsl.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Optional;

public final class ClasspathDescriptorSource implements DescriptorSource {

    @Override
    public String name() {
        return "the classpath";
    }

    @Override
    public Optional<byte[]> read(String location) throws IOException {
        try (InputStream resource = ClasspathDescriptorSource.class.getResourceAsStream(location)) {
            if (resource == null) return Optional.empty();
            return Optional.of(resource.readAllBytes());
        }
    }

    @Override
    public String resolve(String from, String relative) {
        return URI.create(from).resolve(relative).getPath();
    }
}
