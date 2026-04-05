package software.spool.dsl.descriptors.module.ingester;

public record IngesterDescriptor(
        String id,
        FlushIngesterDescriptor flush
) {
}
