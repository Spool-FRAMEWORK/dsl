package software.spool.dsl.descriptors.module.crawler.source.poll;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Map;

public record PollSourceDescriptor(
        Http http,
        File file,
        DataBase database,
        Custom custom,
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

    public record Http(String url) {}
    public record File(String path) {}
    public record DataBase(String type, String host, String database, String user, String password, String query) {}
    public record Custom(String pluginName, Map<String, String> configuration) {}
}