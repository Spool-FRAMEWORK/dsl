package software.spool.dsl.source;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reads descriptors from the classpath, by resource path such as {@code /Spool.yaml}. A folder is listed both when it
 * is a directory, as in development, and when it is inside a jar.
 */
public final class ClasspathDescriptorSource implements DescriptorSource {
    private static final String JAR = "jar";

    private final ClassLoader classLoader;

    public ClasspathDescriptorSource() {
        this(ClasspathDescriptorSource.class.getClassLoader());
    }

    public ClasspathDescriptorSource(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public String name() {
        return "the classpath";
    }

    @Override
    public Optional<byte[]> read(String location) throws IOException {
        try (InputStream resource = classLoader.getResourceAsStream(withoutLeadingSlash(location))) {
            if (resource == null) return Optional.empty();
            return Optional.of(resource.readAllBytes());
        }
    }

    @Override
    public Optional<List<String>> list(String folder) throws IOException {
        URL url = classLoader.getResource(withoutLeadingSlash(folder));
        if (url == null) return Optional.empty();
        URI uri = toUri(url);
        if (!JAR.equals(uri.getScheme())) return Optional.of(resourcesUnder(Path.of(uri), folder));
        return Optional.of(resourcesInJar(uri, folder));
    }

    @Override
    public String resolve(String from, String relative) {
        return URI.create(from).resolve(relative).getPath();
    }

    private static List<String> resourcesUnder(Path root, String folder) throws IOException {
        String prefix = folder.endsWith("/") ? folder : folder + "/";
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .map(file -> prefix + root.relativize(file).toString().replace('\\', '/'))
                    .toList();
        }
    }

    /** A jar someone else already opened is listed as it is and left open; one opened here is closed after. */
    private static List<String> resourcesInJar(URI uri, String folder) throws IOException {
        try {
            return resourcesUnder(FileSystems.getFileSystem(uri).getPath(folder), folder);
        } catch (FileSystemNotFoundException notOpen) {
            try (FileSystem jar = FileSystems.newFileSystem(uri, Map.of())) {
                return resourcesUnder(jar.getPath(folder), folder);
            }
        }
    }

    private static URI toUri(URL url) throws IOException {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new IOException("The classpath folder " + url + " has no valid URI", e);
        }
    }

    private static String withoutLeadingSlash(String location) {
        return location.startsWith("/") ? location.substring(1) : location;
    }
}
