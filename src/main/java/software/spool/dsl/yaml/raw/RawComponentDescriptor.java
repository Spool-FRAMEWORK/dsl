package software.spool.dsl.yaml.raw;

import java.util.Map;

public record RawComponentDescriptor(
        String type,
        Map<String, String> configuration
) {}