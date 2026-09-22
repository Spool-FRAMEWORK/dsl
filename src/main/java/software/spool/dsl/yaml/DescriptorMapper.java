package software.spool.dsl.yaml;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.yaml.raw.RawComponentDescriptor;
import software.spool.dsl.yaml.raw.RawInfrastructureDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.PluginResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class DescriptorMapper {
    private DescriptorMapper() {}

    public static SpoolNodeDescriptor map(RawSpoolNodeDescriptor raw) {
        return new SpoolNodeDescriptor(
                toInfrastructure(raw.infrastructure()),
                toModules(raw.modules())
        );
    }

    private static InfrastructureDescriptor toInfrastructure(RawInfrastructureDescriptor raw) {
        DescriptorReader.require("infrastructure", raw);
        return new InfrastructureDescriptor(
                raw.watchdog(),
                toComponent("infrastructure.eventBus", raw.eventBus()),
                toComponent("infrastructure.inbox", raw.inbox()),
                toComponent("infrastructure.dataLake", raw.dataLake()),
                toComponent("infrastructure.quarantineStore", raw.quarantineStore())
        );
    }

    /**
     * A component that is not in the descriptor is left out: only the modules that use it need it, and
     * {@code InfrastructurePluginFactory} asks for it at that point.
     */
    private static InfrastructureComponentDescriptor toComponent(String path, RawComponentDescriptor raw) {
        if (raw == null) return null;
        return new InfrastructureComponentDescriptor(
                DescriptorReader.require(path + ".type", raw.type()),
                raw.configuration() != null ? raw.configuration() : Map.of()
        );
    }

    private static List<SpoolModuleDescriptor> toModules(List<Map<String, Object>> entries) {
        List<Map<String, Object>> modules = DescriptorReader.require("modules", entries);
        List<SpoolModuleDescriptor> descriptors = new ArrayList<>();
        for (int i = 0; i < modules.size(); i++) {
            descriptors.add(toModule(DescriptorReader.of("modules[" + i + "]", modules.get(i))));
        }
        return descriptors;
    }

    /** Each entry names its module type with its only key; the provider registered for that name reads it. */
    private static SpoolModuleDescriptor toModule(DescriptorReader entry) {
        Set<String> keys = entry.keys();
        if (keys.size() != 1) {
            throw new InvalidDescriptorException(entry.path(),
                    "must have exactly one key naming the module type, found " + keys);
        }
        String type = keys.iterator().next();
        return providerFor(entry.path(), type).read(entry.objectOrEmpty(type));
    }

    private static ModuleDescriptorProvider providerFor(String path, String type) {
        try {
            return PluginResolver.get(ModuleDescriptorProvider.class, type);
        } catch (IllegalStateException unknown) {
            String known = PluginRegistry.findAll(ModuleDescriptorProvider.class).keySet().stream()
                    .map(String::toLowerCase).sorted().collect(Collectors.joining(", "));
            throw new InvalidDescriptorException(path,
                    "has an unknown module type '" + type + "'. Known types: " + known);
        }
    }
}
