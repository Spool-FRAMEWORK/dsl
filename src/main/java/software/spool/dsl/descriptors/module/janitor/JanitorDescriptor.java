package software.spool.dsl.descriptors.module.janitor;

import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.util.Map;

public record JanitorDescriptor(
        String id,
        Map<String, String> configuration
) implements SpoolModuleDescriptor {
    @Override public String moduleType() {
        return "JANITOR";
    }
}
