package software.spool.dsl.descriptors.infrastructure;

import software.spool.dsl.Descriptor;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;

/** A named piece of the infrastructure: its kind, the plugin that builds it and that plugin's configuration. */
public record InfrastructureComponentDescriptor(
        String name,
        InfrastructureKind kind,
        String type,
        Map<String, String> configuration
) implements Descriptor {
    public String pluginName() { return type(); }

    public PluginConfiguration toPluginConfiguration() {
        return PluginConfiguration.of(configuration());
    }

    /** Where the piece is in the descriptor, for the messages that point at it. */
    public String path() {
        return "infrastructure." + name;
    }
}
