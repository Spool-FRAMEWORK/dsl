package software.spool.dsl.source;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * A descriptor that arrives as text, such as the body of a request, at the location {@value #LOCATION}. It has no
 * other files, so an {@code include:} of it is not found.
 */
public final class TextDescriptorSource implements DescriptorSource {
    public static final String LOCATION = "descriptor.spool.yaml";

    private final byte[] text;

    public TextDescriptorSource(String text) {
        this.text = text.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String name() {
        return "the text of the descriptor";
    }

    @Override
    public Optional<byte[]> read(String location) {
        return location.equals(LOCATION) ? Optional.of(text.clone()) : Optional.empty();
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
