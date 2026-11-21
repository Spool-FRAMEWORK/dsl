package software.spool.dsl.descriptors.module.crawler;

import software.spool.core.model.Event;
import software.spool.core.port.serde.NamingConvention;

import java.util.List;

public record EventMappingDescriptor(
        NamingConvention namingConvention,
        List<Class<? extends Event>> domainEvents,
        List<String> attributeList
) {}
