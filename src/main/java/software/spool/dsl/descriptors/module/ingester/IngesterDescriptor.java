package software.spool.dsl.descriptors.module.ingester;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.util.Map;

public record IngesterDescriptor(
        String type,
        String id,
        Map<String, String> configuration
) implements SpoolModuleDescriptor {
    @Override public String moduleType() {
        return "INGESTER_" + type;
    }
}
