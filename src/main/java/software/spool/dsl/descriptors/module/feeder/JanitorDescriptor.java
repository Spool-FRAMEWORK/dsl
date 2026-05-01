package software.spool.dsl.descriptors.module.feeder;

public record JanitorDescriptor(
        String id,
        Integer workers,
        PollFeederDescriptor poll
) {
}
