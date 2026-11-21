package software.spool.dsl;

import software.spool.dsl.source.ClasspathDescriptorSource;
import software.spool.dsl.source.DescriptorSource;
import software.spool.dsl.source.FileSystemDescriptorSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * The descriptors of a folder and its subfolders: every file whose name ends in {@value #NODE_SUFFIX} is a node, and
 * any other file, like one that is only included, is not. Each is loaded like a single descriptor, with includes,
 * profile and variables, and no two of them can open the same port.
 */
public final class DescriptorFolder {
    public static final String NODE_SUFFIX = ".spool.yaml";

    private final DescriptorSource source;
    private final DescriptorLoader loader;

    public DescriptorFolder(DescriptorSource source, UnaryOperator<String> environment) {
        this.source = source;
        this.loader = new DescriptorLoader(source, environment);
    }

    public static DescriptorFolder fromFileSystem() {
        return new DescriptorFolder(new FileSystemDescriptorSource(), System::getenv);
    }

    public static DescriptorFolder fromClasspath() {
        return new DescriptorFolder(new ClasspathDescriptorSource(), System::getenv);
    }

    /** The descriptors of the folder, named by their path relative to it and in the order of those names. */
    public List<NamedDescriptor> load(String folder, Optional<String> profile) throws IOException {
        List<String> locations = source.list(folder)
                .orElseThrow(() -> new FileNotFoundException("Folder not found in " + source.name() + ": " + folder));
        List<NamedDescriptor> descriptors = new ArrayList<>();
        for (String location : locations.stream().filter(DescriptorFolder::isNode).toList()) {
            descriptors.add(new NamedDescriptor(relativeTo(folder, location), loader.load(location, profile, Function.identity())));
        }
        if (descriptors.isEmpty()) {
            throw new IOException("Folder " + folder + " in " + source.name()
                    + " has no descriptor: the descriptors of a folder end in " + NODE_SUFFIX);
        }
        descriptors.sort(Comparator.comparing(NamedDescriptor::name));
        DistinctPorts.check(descriptors);
        return descriptors;
    }

    public Optional<String> profileOfTheEnvironment() {
        return loader.profileOfTheEnvironment();
    }

    private static boolean isNode(String location) {
        return location.endsWith(NODE_SUFFIX);
    }

    private static String relativeTo(String folder, String location) {
        String normalizedFolder = folder.replace('\\', '/');
        String normalizedLocation = location.replace('\\', '/');
        String prefix = normalizedFolder.endsWith("/") ? normalizedFolder : normalizedFolder + "/";
        return normalizedLocation.startsWith(prefix) ? normalizedLocation.substring(prefix.length()) : normalizedLocation;
    }
}
