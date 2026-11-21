package software.spool.dsl.descriptors.module.mounter;

/** The partition of a written record: the one it comes with, or one built from its attributes. */
public enum PartitioningType {
    SAME_AS_SOURCE,
    ATTRIBUTES
}
