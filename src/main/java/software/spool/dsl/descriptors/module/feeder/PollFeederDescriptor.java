package software.spool.dsl.descriptors.module.feeder;

public record PollFeederDescriptor(
        Integer every,
        String unit
) {
}
