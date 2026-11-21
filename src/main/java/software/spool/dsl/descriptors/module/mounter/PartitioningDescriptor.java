package software.spool.dsl.descriptors.module.mounter;

import java.util.List;

/** How a mounter decides the partition of each record it writes. Only {@code ATTRIBUTES} has attributes. */
public record PartitioningDescriptor(PartitioningType type, List<String> attributes) {
}
