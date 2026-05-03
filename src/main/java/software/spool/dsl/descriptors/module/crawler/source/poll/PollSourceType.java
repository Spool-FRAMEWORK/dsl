package software.spool.dsl.descriptors.module.crawler.source.poll;

import java.util.Arrays;

public enum PollSourceType {
    HTTP("http"),
    DATABASE("database"),
    FILE("file"),
    CUSTOM("custom");

    private final String fieldName;

    PollSourceType(String fieldName) {
        this.fieldName = fieldName;
    }

    public static PollSourceType fromValue(String fieldName) {
        return Arrays.stream(values())
                .filter(type -> type.fieldName.equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown inbox field: " + fieldName));
    }
}
