package software.spool.dsl.descriptors.module.janitor;

public record JanitorDescriptor(
        String id,
        Integer workers,
        Integer everyMilliseconds
) {
}
