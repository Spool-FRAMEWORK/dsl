package software.spool.dsl.descriptors.module.ingester;

public record FlushIngesterDescriptor(
        Integer size,
        Integer everyMilliseconds
) {
}
