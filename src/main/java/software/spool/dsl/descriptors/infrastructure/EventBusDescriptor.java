package software.spool.dsl.descriptors.infrastructure;

import java.util.Arrays;

public record EventBusDescriptor(
        EventBusType type,
        String url
) {
    public static enum EventBusType {
        IN_MEMORY("inMemory"),
        KAFKA("kafka");

        private final String value;

        EventBusType(String value) {
            this.value = value;
        }

        public static EventBusType fromValue(String value) {
            return Arrays.stream(values())
                    .filter(t -> t.value.equals(value))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown value: " + value));
        }
    }
}
