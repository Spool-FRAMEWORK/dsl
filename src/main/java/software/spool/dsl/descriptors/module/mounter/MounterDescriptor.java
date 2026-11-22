package software.spool.dsl.descriptors.module.mounter;

import software.spool.core.model.schedule.Schedule;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.mounter.api.MountMode;

import java.util.Optional;

/** A mounter, which fills one data mart from partitions of the data lake. Its components are the keys of its block. */
public record MounterDescriptor(
        String id,
        MounterType type,
        Optional<Schedule> schedule,
        MountMode mode,
        boolean validate,
        Optional<AggregatorClass> aggregator,
        MountReadDescriptor read,
        MountWriteDescriptor write
) implements SpoolModuleDescriptor {
    @Override public String moduleType() {
        return "MOUNTER";
    }
}
