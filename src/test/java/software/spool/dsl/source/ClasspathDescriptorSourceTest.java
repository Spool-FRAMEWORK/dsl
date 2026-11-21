package software.spool.dsl.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.dsl.DescriptorFolder;
import software.spool.dsl.NamedDescriptor;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class ClasspathDescriptorSourceTest {
    private static final String INFRASTRUCTURE = "infrastructure:\n  bus: {kind: EVENT_BUS, type: IN_MEMORY}\n";

    @TempDir
    Path folder;

    @Test
    void list_aFolderInsideAJar_givesEveryFileOfItAndItsSubfolders() throws IOException {
        try (URLClassLoader classpath = jarWith(Map.of(
                "pipelines/boe.spool.yaml", "server: {port: 8091}\n" + INFRASTRUCTURE + "modules: []\n",
                "pipelines/bocm/bocm.spool.yaml", "server: {port: 8092}\n" + INFRASTRUCTURE + "modules: []\n",
                "pipelines/shared.yaml", INFRASTRUCTURE))) {

            Optional<List<String>> files = new ClasspathDescriptorSource(classpath).list("/pipelines");

            assertThat(files).hasValueSatisfying(list -> assertThat(list).containsExactlyInAnyOrder(
                    "/pipelines/boe.spool.yaml", "/pipelines/bocm/bocm.spool.yaml", "/pipelines/shared.yaml"));
        }
    }

    @Test
    void load_aFolderInsideAJar_givesANodePerDescriptor() throws IOException {
        try (URLClassLoader classpath = jarWith(Map.of(
                "pipelines/boe.spool.yaml", "server: {port: 8091}\n" + INFRASTRUCTURE + "modules: []\n",
                "pipelines/bocm/bocm.spool.yaml", "server: {port: 8092}\n" + INFRASTRUCTURE + "modules: []\n",
                "pipelines/shared.yaml", INFRASTRUCTURE))) {

            DescriptorFolder descriptors = new DescriptorFolder(new ClasspathDescriptorSource(classpath), name -> null);

            assertThat(descriptors.load("/pipelines", Optional.empty())).extracting(NamedDescriptor::name)
                    .containsExactly("bocm/bocm.spool.yaml", "boe.spool.yaml");
        }
    }

    @Test
    void list_aFolderThatDoesNotExist_givesNothing() throws IOException {
        assertThat(new ClasspathDescriptorSource().list("/no-such-folder")).isEmpty();
    }

    private URLClassLoader jarWith(Map<String, String> entries) throws IOException {
        Path jar = folder.resolve("pipelines.jar");
        try (OutputStream file = Files.newOutputStream(jar); JarOutputStream output = new JarOutputStream(file)) {
            for (String directory : directoriesOf(entries.keySet())) {
                output.putNextEntry(new JarEntry(directory));
                output.closeEntry();
            }
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                output.putNextEntry(new JarEntry(entry.getKey()));
                output.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                output.closeEntry();
            }
        }
        return new URLClassLoader(new URL[]{jar.toUri().toURL()}, null);
    }

    /** The folders of the entries, as a build tool writes them into a jar. */
    private static Set<String> directoriesOf(Set<String> entries) {
        Set<String> directories = new LinkedHashSet<>();
        for (String entry : entries) {
            for (int slash = entry.indexOf('/'); slash >= 0; slash = entry.indexOf('/', slash + 1)) {
                directories.add(entry.substring(0, slash + 1));
            }
        }
        return directories;
    }
}
