package software.spool.dsl.yaml.raw;

import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;

import java.util.Map;

public record RawComponentDescriptor(
        String type,
        Map<String, String> configuration
) {
    private static final String TYPE = "type";
    private static final String CONFIGURATION = "configuration";

    static RawComponentDescriptor from(DescriptorReader component) {
        component.knownKeysOnly(RawComponentDescriptor.class);
        return new RawComponentDescriptor(
                component.optional(TYPE, ValueParsers.STRING).orElse(null),
                component.stringMap(CONFIGURATION)
        );
    }
}
