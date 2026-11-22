package software.spool.dsl.providers.crawler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.ToolProvider;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventClassesTest {
    private static final String ORDER_PLACED = """
            package userevents;

            public record OrderPlaced(String eventId, String causationId, String correlationId, java.time.Instant timestamp)
                    implements software.spool.core.model.Event {
            }
            """;

    @TempDir
    Path folder;

    @Test
    void resolve_classOnlyInTheContextClassLoader_isFound() throws Exception {
        Path source = Files.writeString(Files.createDirectories(folder.resolve("src/userevents")).resolve("OrderPlaced.java"), ORDER_PLACED);
        Path classes = Files.createDirectories(folder.resolve("classes"));
        ToolProvider.getSystemJavaCompiler().run(null, null, null, "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString());
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try (URLClassLoader project = new URLClassLoader(new URL[]{classes.toUri().toURL()}, previous)) {
            thread.setContextClassLoader(project);

            assertThat(EventClasses.in("userevents").resolve("modules[0].crawler", List.of("OrderPlaced")))
                    .extracting(Class::getName).containsExactly("userevents.OrderPlaced");
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
