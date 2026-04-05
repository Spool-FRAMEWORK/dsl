package software.spool.dsl.descriptors.module.crawler.source;

import java.util.Arrays;

public enum SourceType {
    POLL("poll"),
    STREAM("stream"),
    WEBHOOK("webhook");

    private final String fieldName;

    SourceType(String fieldName) {
        this.fieldName = fieldName;
    }

    public static SourceType fromFieldName(String fieldName) {
        return Arrays.stream(values())
                .filter(type -> type.fieldName.equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + fieldName));
    }
}
