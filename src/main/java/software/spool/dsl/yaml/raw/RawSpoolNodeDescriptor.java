package software.spool.dsl.yaml.raw;

import java.util.List;
import java.util.Map;

public record RawSpoolNodeDescriptor(
        RawInfrastructureDescriptor infrastructure,
        List<Map<String, Object>> modules
) {}