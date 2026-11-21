package software.spool.dsl;

import software.spool.dsl.providers.authentication.RequestAuthenticators;
import software.spool.core.adapter.otel.OpenTelemetryMetricsRegistry;
import software.spool.core.model.spool.SpoolNode;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.registry.ModuleProviderRegistry;
import software.spool.dsl.source.FileSystemDescriptorSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public abstract class SpoolNodeDSL {
    /** Loads the descriptor from the classpath with the profile in {@value DescriptorLoader#PROFILE_VARIABLE}, if any. */
    public static SpoolNode fromDescriptor(String path) throws IOException {
        DescriptorLoader loader = DescriptorLoader.fromClasspath();
        return loader.load(path, loader.profileOfTheEnvironment(), SpoolNodeDSL::fromDescriptor);
    }

    /** Loads the descriptor from the classpath with the given profile, whatever the environment says. */
    public static SpoolNode fromDescriptor(String path, String profile) throws IOException {
        return DescriptorLoader.fromClasspath().load(path, Optional.of(profile), SpoolNodeDSL::fromDescriptor);
    }

    /** Loads the descriptor from the file system with the profile in {@value DescriptorLoader#PROFILE_VARIABLE}, if any. */
    public static SpoolNode fromDescriptor(Path file) throws IOException {
        DescriptorLoader loader = DescriptorLoader.fromFileSystem();
        return loader.load(FileSystemDescriptorSource.locationOf(file), loader.profileOfTheEnvironment(),
                SpoolNodeDSL::fromDescriptor);
    }

    /** Loads the descriptor from the file system with the given profile, whatever the environment says. */
    public static SpoolNode fromDescriptor(Path file, String profile) throws IOException {
        return DescriptorLoader.fromFileSystem().load(FileSystemDescriptorSource.locationOf(file),
                Optional.of(profile), SpoolNodeDSL::fromDescriptor);
    }

    /** The nodes of every {@value DescriptorFolder#NODE_SUFFIX} in a folder of the file system and its subfolders. */
    public static List<SpoolNode> fromFolder(Path folder) throws IOException {
        DescriptorFolder descriptors = DescriptorFolder.fromFileSystem();
        return nodesOf(descriptors.load(FileSystemDescriptorSource.locationOf(folder), descriptors.profileOfTheEnvironment()));
    }

    /** The nodes of every {@value DescriptorFolder#NODE_SUFFIX} in a folder of the classpath, also inside a jar. */
    public static List<SpoolNode> fromFolder(String folder) throws IOException {
        DescriptorFolder descriptors = DescriptorFolder.fromClasspath();
        return nodesOf(descriptors.load(folder, descriptors.profileOfTheEnvironment()));
    }

    private static List<SpoolNode> nodesOf(List<NamedDescriptor> descriptors) {
        return descriptors.stream().map(named -> fromDescriptor(named.descriptor())).toList();
    }

    public static SpoolNode fromDescriptor(SpoolNodeDescriptor descriptor) {
        RequestAuthenticators authentication = RequestAuthenticators.from(descriptor.authentication());
        SpoolNode node = SpoolNode.create(descriptor.server().port(), new OpenTelemetryMetricsRegistry());
        descriptor.declaredModules().stream()
                .map(module -> ModuleProviderRegistry.build(module.descriptor(), descriptor.infrastructureOf(module), authentication))
                .forEach(node::register);
        return node;
    }
}
