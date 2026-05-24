package software.spool.dsl.descriptors.infrastructure;

import software.spool.dsl.Descriptor;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;

public record InfrastructureComponentDescriptor(String type, Map<String, String> configuration) implements Descriptor {
    public String pluginName() { return type(); }
    public PluginConfiguration toPluginConfiguration() {
        return PluginConfiguration.of(configuration());
    }
}