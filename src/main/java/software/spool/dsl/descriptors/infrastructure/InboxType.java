package software.spool.dsl.descriptors.infrastructure;

import java.util.Arrays;

public enum InboxType {
    SQL("sql"),
    S3("s3"),
    IN_MEMORY("inMemory"),
    CUSTOM("custom");

    private final String fieldName;

    InboxType(String fieldName) {
        this.fieldName = fieldName;
    }

    public static InboxType fromFieldName(String fieldName) {
        return Arrays.stream(values())
                .filter(type -> type.fieldName.equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown inbox field: " + fieldName));
    }
}
