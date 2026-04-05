package software.spool.dsl.descriptors.module;

import java.util.Arrays;

public enum SpoolModuleType {
    CRAWLER("crawler"),
    FEEDER("feeder"),
    INGESTER("ingester");

    private final String fieldName;

    private SpoolModuleType(final String fieldName) {
        this.fieldName = fieldName;
    }

    public static SpoolModuleType fromFieldName(String fieldName) {
        return Arrays.stream(values())
                .filter(type -> type.fieldName.equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + fieldName));
    }
}
