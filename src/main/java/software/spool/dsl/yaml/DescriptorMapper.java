package software.spool.dsl.yaml;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.DeclaredModule;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.authentication.AuthenticationDescriptor;
import software.spool.dsl.descriptors.infrastructure.DeclaredInfrastructure;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;
import software.spool.dsl.yaml.raw.RawComponentDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.PluginResolver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class DescriptorMapper {
    private static final String USES = "uses";

    private DescriptorMapper() {}

    public static SpoolNodeDescriptor map(RawSpoolNodeDescriptor raw) {
        DeclaredInfrastructure infrastructure = InfrastructureMapper.map(raw.infrastructure());
        return new SpoolNodeDescriptor(
                Optional.ofNullable(raw.watchdog()),
                infrastructure,
                toModules(raw.modules(), infrastructure),
                toAuthentication(raw.authentication())
        );
    }

    private static Map<String, AuthenticationDescriptor> toAuthentication(Map<String, RawComponentDescriptor> raw) {
        if (raw == null) return Map.of();
        Map<String, AuthenticationDescriptor> providers = new LinkedHashMap<>();
        raw.forEach((name, provider) -> providers.put(name, new AuthenticationDescriptor(
                DescriptorReader.require("authentication." + name + ".type", provider.type()),
                provider.configuration() != null ? provider.configuration() : Map.of())));
        return providers;
    }

    private static List<DeclaredModule> toModules(List<Map<String, Object>> entries, DeclaredInfrastructure infrastructure) {
        List<Map<String, Object>> modules = DescriptorReader.require("modules", entries);
        List<DeclaredModule> declared = new ArrayList<>();
        for (int i = 0; i < modules.size(); i++) {
            declared.add(toModule(DescriptorReader.of("modules[" + i + "]", modules.get(i)), infrastructure));
        }
        return declared;
    }

    /**
     * Each entry names its module type with its only key; the provider registered for that name reads it. The pieces
     * in {@code uses} are read here, since every module has them and no provider knows them.
     */
    private static DeclaredModule toModule(DescriptorReader entry, DeclaredInfrastructure infrastructure) {
        Set<String> keys = entry.keys();
        if (keys.size() != 1) {
            throw new InvalidDescriptorException(entry.path(),
                    "must have exactly one key naming the module type, found " + keys);
        }
        String type = keys.iterator().next();
        DescriptorReader module = entry.objectOrEmpty(type);
        List<String> uses = module.optional(USES, ValueParsers.TEXTS).orElse(List.of());
        return new DeclaredModule(module.path(), providerFor(entry.path(), type).read(module.without(USES)),
                infrastructure.named(module.path() + "." + USES, uses));
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
