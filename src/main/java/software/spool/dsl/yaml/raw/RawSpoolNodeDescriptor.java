package software.spool.dsl.yaml.raw;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record RawSpoolNodeDescriptor(
        String watchdog,
        RawInfrastructureDescriptor infrastructure,
        List<Map<String, Object>> modules,
        Map<String, RawComponentDescriptor> authentication
) {
    private static final String WATCHDOG = "watchdog";
    private static final String INFRASTRUCTURE = "infrastructure";
    private static final String MODULES = "modules";
    private static final String AUTHENTICATION = "authentication";

    public RawSpoolNodeDescriptor(RawInfrastructureDescriptor infrastructure, List<Map<String, Object>> modules) {
        this(null, infrastructure, modules, null);
    }

    /** Reads the descriptor from its YAML tree, failing on any key it does not know. */
    public static RawSpoolNodeDescriptor from(Map<String, Object> tree) {
        DescriptorReader root = DescriptorReader.of("", tree).knownKeysOnly(RawSpoolNodeDescriptor.class);
        return new RawSpoolNodeDescriptor(
                root.optional(WATCHDOG, ValueParsers.STRING).orElse(null),
                root.value(INFRASTRUCTURE).map(value -> RawInfrastructureDescriptor.from(root.object(INFRASTRUCTURE)))
                        .orElse(null),
                root.value(MODULES).map(RawSpoolNodeDescriptor::modulesOf).orElse(null),
                root.value(AUTHENTICATION).map(value -> providersOf(root.object(AUTHENTICATION))).orElse(null)
        );
    }

    private static Map<String, RawComponentDescriptor> providersOf(DescriptorReader block) {
        Map<String, RawComponentDescriptor> providers = new LinkedHashMap<>();
        for (String name : block.keys()) providers.put(name, RawComponentDescriptor.from(block.object(name)));
        return providers;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> modulesOf(Object value) {
        if (!(value instanceof List<?> entries)) {
            throw new InvalidDescriptorException(MODULES, "must be a list, got '" + value + "'");
        }
        List<Map<String, Object>> modules = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            Object entry = entries.get(i);
            if (entry != null && !(entry instanceof Map<?, ?>)) {
                throw new InvalidDescriptorException(MODULES + "[" + i + "]", "must be an object, got '" + entry + "'");
            }
            modules.add((Map<String, Object>) entry);
        }
        return modules;
    }
}
