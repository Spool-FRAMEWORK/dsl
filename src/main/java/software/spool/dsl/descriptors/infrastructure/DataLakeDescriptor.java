package software.spool.dsl.descriptors.infrastructure;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public record DataLakeDescriptor(
        InMemory inMemory,
        Sql sql,
        Custom custom
) {
    public DataLakeDescriptor {
        if (Stream.of(sql, inMemory, custom).filter(Objects::nonNull).count() != 1)
            throw new IllegalArgumentException("DataLake descriptor must contain exactly one configuration");
    }

    public DataLakeType type() {
        String fieldName = Arrays.stream(getClass().getRecordComponents())
                .filter(component -> valueOf(component) != null)
                .map(RecordComponent::getName)
                .findFirst()
                .orElseThrow();

        return DataLakeType.fromFieldName(fieldName);
    }

    private Object valueOf(RecordComponent component) {
        try {
            return component.getAccessor().invoke(this);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot inspect inbox descriptor", e);
        }
    }

    public static record InMemory() {}
    public static record Sql(String type, String host, String database, String user, String password) {}
    public static record Custom(String implementationClass) {}
}