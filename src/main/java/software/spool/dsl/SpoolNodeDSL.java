package software.spool.dsl;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.model.spool.SpoolNode;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.registry.ModuleProviderRegistry;
import software.spool.dsl.yaml.DescriptorMapper;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Objects;

public abstract class SpoolNodeDSL {
    public static SpoolNode fromDescriptor(String path) throws IOException {
        try (BufferedInputStream is = new BufferedInputStream(
                Objects.requireNonNull(SpoolNodeDSL.class.getResourceAsStream(path), "Resource not found: " + path))) {
            RawSpoolNodeDescriptor raw = PayloadDeserializerFactory.yaml()
                    .as(RawSpoolNodeDescriptor.class)
                    .deserialize(is.readAllBytes());
            return fromDescriptor(DescriptorMapper.map(raw));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static SpoolNode fromDescriptor(SpoolNodeDescriptor descriptor) {
        SpoolNode node = SpoolNode.create(new OpenTelemetryMetricsRegistry());
        descriptor.modules().stream()
                .map(m -> ModuleProviderRegistry.build(m, descriptor.infrastructure()))
                .forEach(node::register);
        return node;
    }
}