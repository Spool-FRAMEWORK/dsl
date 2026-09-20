package software.spool.dsl;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.model.spool.SpoolNode;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.registry.ModuleProviderRegistry;
import software.spool.dsl.yaml.DescriptorMapper;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public abstract class SpoolNodeDSL {
    public static SpoolNode fromDescriptor(String path) throws IOException {
        InputStream resource = SpoolNodeDSL.class.getResourceAsStream(path);
        if (resource == null) throw new FileNotFoundException("Descriptor not found in the classpath: " + path);
        try (BufferedInputStream is = new BufferedInputStream(resource)) {
            RawSpoolNodeDescriptor raw = PayloadDeserializerFactory.yaml()
                    .as(RawSpoolNodeDescriptor.class)
                    .deserialize(is.readAllBytes());
            return fromDescriptor(DescriptorMapper.map(raw));
        } catch (InvalidDescriptorException e) {
            throw new IOException("Invalid descriptor " + path + ": " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IOException("Could not load descriptor " + path + ": " + e.getMessage(), e);
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