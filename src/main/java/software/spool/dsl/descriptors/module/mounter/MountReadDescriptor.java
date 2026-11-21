package software.spool.dsl.descriptors.module.mounter;

import software.spool.core.model.vo.PartitionKey;

import java.util.List;

/** What a mounter reads: partitions of the data lake, written as paths such as {@code source=payments/eventType=Payment}. */
public record MountReadDescriptor(List<PartitionKey> partitions) {
    public PartitionKey partition() {
        return partitions.getFirst();
    }
}
