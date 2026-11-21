package software.spool.dsl;

import software.spool.dsl.descriptors.SpoolNodeDescriptor;

/** A loaded descriptor and the name it is reported by: its path relative to its folder, or its own location. */
public record NamedDescriptor(String name, SpoolNodeDescriptor descriptor) {}
