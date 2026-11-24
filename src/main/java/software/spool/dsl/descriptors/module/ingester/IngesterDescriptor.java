package software.spool.dsl.descriptors.module.ingester;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * An ingester. A {@code FLUSH} one writes a batch when it reaches {@code size} envelopes or, if set, {@code every}.
 * {@code processedKeysWindow} is how long it remembers the keys it wrote; without it, the ingester's default.
 */
public record IngesterDescriptor(
        IngesterType type,
        String id,
        Optional<Integer> size,
        Optional<Duration> every,
        Optional<Duration> processedKeysWindow
) implements SpoolModuleDescriptor {
    public IngesterDescriptor {
        Objects.requireNonNull(type, "type is required");
    }

    /** An ingester of the given type that writes with the default batch. */
    public static IngesterDescriptor withDefaults(IngesterType type, String id) {
        return new IngesterDescriptor(type, id, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Override public String moduleType() {
        return "INGESTER_" + type;
    }
}
