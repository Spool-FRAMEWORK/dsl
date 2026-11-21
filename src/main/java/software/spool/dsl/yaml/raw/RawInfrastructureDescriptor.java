package software.spool.dsl.yaml.raw;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.reader.DescriptorReader;

import java.util.LinkedHashMap;
import java.util.Map;

/** The pieces of {@code infrastructure} by name, as they are written. */
public record RawInfrastructureDescriptor(Map<String, RawPieceDescriptor> pieces) {
    private static final String WATCHDOG = "watchdog";

    static RawInfrastructureDescriptor from(DescriptorReader infrastructure) {
        infrastructure.value(WATCHDOG).ifPresent(url -> {
            throw new InvalidDescriptorException(infrastructure.path() + "." + WATCHDOG,
                    "is no longer supported: write watchdog: \"" + url + "\" at the root of the descriptor");
        });
        Map<String, RawPieceDescriptor> pieces = new LinkedHashMap<>();
        for (String name : infrastructure.keys()) pieces.put(name, RawPieceDescriptor.from(infrastructure.object(name)));
        return new RawInfrastructureDescriptor(pieces);
    }
}
