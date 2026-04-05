package software.spool.dsl.descriptors.infrastructure;

import java.util.Arrays;

public enum DataLakeType {
    SQL("sql"),
    IN_MEMORY("inMemory"),
    CUSTOM("custom");

    private final String fieldName;

    DataLakeType(String fieldName) {
        this.fieldName = fieldName;
    }

    public static DataLakeType fromFieldName(String fieldName) {
        return Arrays.stream(values())
                .filter(type -> type.fieldName.equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown inbox field: " + fieldName));
    }
}
