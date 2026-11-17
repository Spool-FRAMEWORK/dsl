package software.spool.dsl.compose;

import java.util.Map;

/** A descriptor with its includes combined and its profile applied, and where each of its values comes from. */
public record ComposedDescriptor(Map<String, Object> tree, DescriptorOrigins origins) {}
