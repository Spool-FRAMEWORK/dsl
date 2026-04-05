package software.spool.dsl.descriptors.module.feeder;

public record FeederDescriptor(
        String id,
        Integer workers,
        PollFeederDescriptor poll
) {
}
