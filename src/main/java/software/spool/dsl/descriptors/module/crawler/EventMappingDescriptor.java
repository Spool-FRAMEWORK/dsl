package software.spool.dsl.descriptors.module.crawler;

import software.spool.core.port.serde.NamingConvention;

import java.util.List;
import java.util.Objects;

public record EventMappingDescriptor(
        NamingConvention namingConvention,
        List<PartitionAttributeDescriptor> attributeList
) {
    public EventMappingDescriptor(NamingConvention namingConvention, List<PartitionAttributeDescriptor> attributeList) {
        this.namingConvention = namingConvention;
        this.attributeList = Objects.requireNonNullElse(attributeList, List.of());
    }
}
