package software.spool.dsl.providers.mounter;

import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.module.ScheduleReader;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.mounter.AggregatorClass;
import software.spool.dsl.descriptors.module.mounter.MountReadDescriptor;
import software.spool.dsl.descriptors.module.mounter.MountWriteDescriptor;
import software.spool.dsl.descriptors.module.mounter.MounterDescriptor;
import software.spool.dsl.descriptors.module.mounter.MounterType;
import software.spool.dsl.descriptors.module.mounter.PartitioningDescriptor;
import software.spool.dsl.descriptors.module.mounter.PartitioningType;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.mounter.api.MountMode;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class MounterModuleDescriptorProvider implements ModuleDescriptorProvider {
    private static final ScheduleReader SCHEDULE = ScheduleReader.standard();
    private static final String PARTITION_PATH_SEPARATOR = "/";

    @Override
    public String name() {
        return "MOUNTER";
    }

    @Override
    public SpoolModuleDescriptor read(DescriptorReader mounter) {
        String id = mounter.string("id");
        try {
            return readMounter(id, mounter.knownKeysOnly(MounterDescriptor.class));
        } catch (InvalidDescriptorException e) {
            throw e.inModule(id);
        }
    }

    private static MounterDescriptor readMounter(String id, DescriptorReader mounter) {
        MounterType type = mounter.required("type", ValueParsers.oneOf(MounterType.class));
        Optional<Schedule> schedule = SCHEDULE.read(mounter);
        if (type == MounterType.POLL && schedule.isEmpty()) {
            throw new InvalidDescriptorException(mounter.path() + ".schedule", "is required");
        }
        MountMode mode = mounter.required("mode", ValueParsers.oneOf(MountMode.class));
        boolean validate = mounter.optional("validate", ValueParsers.BOOLEAN).orElse(false);
        if (validate && mode != MountMode.TRANSFORMATION) {
            throw new InvalidDescriptorException(mounter.path() + ".validate",
                    "is not allowed in a " + mode + " mount: validation only applies to TRANSFORMATION, from bronze to silver");
        }
        return new MounterDescriptor(
                id,
                type,
                schedule,
                mode,
                validate,
                mounter.optional("aggregator", ValueParsers.STRING)
                        .map(className -> AggregatorClass.named(mounter.path() + ".aggregator", className)),
                mountRead(mounter.object("read").knownKeysOnly(MountReadDescriptor.class)),
                mountWrite(mounter.object("write").knownKeysOnly(MountWriteDescriptor.class))
        );
    }

    private static MountReadDescriptor mountRead(DescriptorReader read) {
        List<String> partitions = read.required("partitions", ValueParsers.TEXTS);
        if (partitions.size() != 1) {
            throw new InvalidDescriptorException(read.path() + ".partitions",
                    "must have exactly one partition, got " + partitions.size());
        }
        return new MountReadDescriptor(partitions.stream().map(MounterModuleDescriptorProvider::partitionOf).toList());
    }

    private static MountWriteDescriptor mountWrite(DescriptorReader write) {
        return new MountWriteDescriptor(write.string("dataMart"),
                partitioning(write.objectOrEmpty("partitioning").knownKeysOnly(PartitioningDescriptor.class)));
    }

    private static PartitioningDescriptor partitioning(DescriptorReader partitioning) {
        PartitioningType type = partitioning.required("type", ValueParsers.oneOf(PartitioningType.class));
        List<String> attributes = type == PartitioningType.ATTRIBUTES
                ? partitioning.required("attributes", ValueParsers.TEXTS)
                : List.of();
        return new PartitioningDescriptor(type, attributes);
    }

    private static PartitionKey partitionOf(String path) {
        return new PartitionKey(KeyEntries.join(Arrays.asList(path.split(PARTITION_PATH_SEPARATOR))));
    }
}
