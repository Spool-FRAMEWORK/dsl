package software.spool.dsl.descriptors.authentication;

import java.util.Map;

/** A provider of the {@code authentication} block: the plugin that identifies callers and its configuration (ADR-013). */
public record AuthenticationDescriptor(String type, Map<String, String> configuration) {
    public AuthenticationDescriptor {
        configuration = Map.copyOf(configuration);
    }
}
