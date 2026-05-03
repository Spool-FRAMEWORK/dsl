package software.spool.dsl;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.spool.SpoolModule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.dsl.builder.CrawlerBuilder;
import software.spool.dsl.builder.JanitorBuilder;
import software.spool.dsl.builder.IngesterBuilder;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class SpoolNodeDSL {
    public static SpoolNode fromDescriptor(String path) throws IOException {
        try(BufferedInputStream is = new BufferedInputStream(
                Objects.requireNonNull(Main.class.getResourceAsStream(path)))) {
            return fromDescriptor(PayloadDeserializerFactory.yaml().as(SpoolNodeDescriptor.class)
                    .deserialize(new String(is.readAllBytes())));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static SpoolNode fromDescriptor(SpoolNodeDescriptor descriptor) throws IOException {
        SpoolNode node = SpoolNode.create();
        buildModulesFrom(descriptor.modules(), descriptor.infrastructure())
                .forEach(node::register);
        return node;
    }

    private static List<SpoolModule> buildModulesFrom(
            List<SpoolModuleDescriptor> moduleDescriptors,
            InfrastructureDescriptor infrastructure
    ) {
        return moduleDescriptors.stream()
                .map(m -> buildModuleFrom(m, infrastructure))
                .toList();
    }

    private static SpoolModule buildModuleFrom(
            SpoolModuleDescriptor moduleDescriptor,
            InfrastructureDescriptor infrastructure
    ) {
        return switch (moduleDescriptor.type()) {
            case CRAWLER -> CrawlerBuilder.buildFrom(moduleDescriptor.crawler(), infrastructure);
            case JANITOR -> JanitorBuilder.buildFrom(moduleDescriptor.janitor(), infrastructure);
            case INGESTER -> IngesterBuilder.buildFrom(moduleDescriptor.ingester(), infrastructure);
        };
    }
}
