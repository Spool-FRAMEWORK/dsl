package software.spool.dsl.descriptors.module.mounter;

/** Where a mounter writes: the data mart it fills and how each record is partitioned in it. */
public record MountWriteDescriptor(String dataMart, PartitioningDescriptor partitioning) {
}
