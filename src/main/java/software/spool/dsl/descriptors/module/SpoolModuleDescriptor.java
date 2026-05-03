package software.spool.dsl.descriptors.module;

import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;

public record SpoolModuleDescriptor(
        CrawlerDescriptor crawler,
        JanitorDescriptor janitor,
        IngesterDescriptor ingester
) {
    public SpoolModuleType type() {
        String fieldName = Arrays.stream(getClass().getRecordComponents())
                .filter(component -> valueOf(component) != null)
                .map(RecordComponent::getName)
                .findFirst()
                .orElseThrow();
        return SpoolModuleType.fromFieldName(fieldName);
    }

    private Object valueOf(RecordComponent component) {
        try {
            return component.getAccessor().invoke(this);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot inspect spool module descriptor", e);
        }
    }
}
