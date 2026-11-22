package software.spool.dsl.descriptors.infrastructure;

import software.spool.core.resilience.model.CircuitBreakerPolicy;
import software.spool.dsl.Descriptor;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;

/**
 * A named piece of the infrastructure: its kind, the plugin that builds it, that plugin's configuration and the
 * circuit that says when the piece stops being called after failing.
 */
public record InfrastructureComponentDescriptor(
        String name,
        InfrastructureKind kind,
        String type,
        Map<String, String> configuration,
        CircuitBreakerPolicy circuit
) implements Descriptor {
    public InfrastructureComponentDescriptor(String name, InfrastructureKind kind, String type,
                                             Map<String, String> configuration) {
        this(name, kind, type, configuration, CircuitBreakerPolicy.defaults());
    }

    public String pluginName() { return type(); }

    public PluginConfiguration toPluginConfiguration() {
        return PluginConfiguration.of(configuration());
    }

    /** Where the piece is in the descriptor, for the messages that point at it. */
    public String path() {
        return "infrastructure." + name;
    }
}
