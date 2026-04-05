package software.spool.dsl.descriptors.module.crawler.source.poll;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;

public record PollSourceDescriptor(
        Http http,
        File file,
        DataBase dataBase,
        ScheduleDescriptor schedule
        ) {
    public PollSourceType type() {
        String fieldName = Arrays.stream(getClass().getRecordComponents())
                .filter(component -> valueOf(component) != null)
                .map(RecordComponent::getName)
                .filter(f -> !f.startsWith("schedule"))
                .findFirst()
                .orElseThrow();

        return PollSourceType.fromValue(fieldName);
    }

    private Object valueOf(RecordComponent component) {
        try {
            return component.getAccessor().invoke(this);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot inspect source descriptor", e);
        }
    }

    public static record Http(String url) {}
    public static record File(String path) {}
    public static record DataBase(String type, String host, String database, String user, String password) {}
}