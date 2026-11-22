package software.spool.dsl.descriptors.module.mounter;

import software.spool.core.model.vo.KeyEntries;
import software.spool.core.model.vo.PartitionKey;

import java.util.List;

/**
 * What a mounter reads: one or more partitions of the data lake, written as paths such as
 * {@code source=payments/eventType=Payment}.
 */
public record MountReadDescriptor(List<PartitionKey> partitions) {
    /** The partition every partition read hangs from: the leading entries they all share, none if they share none. */
    public PartitionKey scope() {
        List<String> shared = KeyEntries.split(partitions.getFirst().value());
        for (PartitionKey partition : partitions) {
            List<String> entries = KeyEntries.split(partition.value());
            int common = 0;
            while (common < shared.size() && common < entries.size() && shared.get(common).equals(entries.get(common))) {
                common++;
            }
            shared = shared.subList(0, common);
        }
        return new PartitionKey(KeyEntries.join(shared));
    }
}
