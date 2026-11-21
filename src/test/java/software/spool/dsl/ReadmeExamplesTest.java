package software.spool.dsl;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Extended;
import software.spool.dsl.providers.KnownPlugins;
import software.spool.infrastructure.spi.Plugin;
import software.spool.infrastructure.spi.provider.ErrorRouterProvider;
import software.spool.infrastructure.spi.provider.PollSourceProvider;
import software.spool.infrastructure.spi.provider.StreamSourceProvider;
import software.spool.infrastructure.spi.provider.authentication.RequestAuthenticatorProvider;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.DataLakeWriterProvider;
import software.spool.infrastructure.spi.provider.inbox.InboxWriterProvider;
import software.spool.infrastructure.spi.provider.quarantine.QuarantineRepositoryProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every YAML example of the README loads, and every plugin it names exists, so the README cannot drift from the
 * descriptor again. A block that starts with a {@code # file.yaml} comment is a file the other examples include.
 * A block that starts with {@code - } is a module, loaded inside a descriptor that has only an event bus.
 */
class ReadmeExamplesTest {
    private static final Path README = Path.of("README.MD");
    private static final Pattern YAML_BLOCK = Pattern.compile("```yaml\\R(.*?)```", Pattern.DOTALL);
    private static final Pattern INCLUDED_FILE = Pattern.compile("\\A# (\\S+\\.ya?ml)\\R");
    private static final String EXAMPLE_VALUE = "example";
    private static final Pattern TABLE_ROW = Pattern.compile("\\|([^|]*)\\|([^|]*)\\|([^|]*)\\|");

    @TempDir
    Path folder;

    @TestFactory
    Stream<DynamicTest> everyExampleOfTheReadmeLoads() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        Matcher block = YAML_BLOCK.matcher(Files.readString(README));
        while (block.find()) {
            String yaml = block.group(1);
            Matcher included = INCLUDED_FILE.matcher(yaml);
            if (included.find()) {
                write(included.group(1), yaml);
                continue;
            }
            tests.add(DynamicTest.dynamicTest("example at offset " + block.start(), () -> {
                SpoolNodeDescriptor node = load(asDescriptor(yaml));
                assertPluginsExist(node);
            }));
        }
        return tests.stream();
    }

    @Test
    void everyTypeOfTheInfrastructureTableIsARegisteredPlugin() throws IOException {
        String table = Files.readString(README).split("## Infrastructure Options")[1].split("\\R\\R")[1];
        String component = "";
        for (String row : table.lines().skip(2).toList()) {
            Matcher cells = TABLE_ROW.matcher(row);
            assertThat(cells.matches()).as("row of the infrastructure table: " + row).isTrue();
            component = cells.group(1).isBlank() ? component : cells.group(1).replace("`", "").strip();
            String type = cells.group(2).replace("`", "").strip();
            InfrastructureComponentDescriptor declared = new InfrastructureComponentDescriptor(type, Map.of());
            switch (component) {
                case "eventBus" -> exists(EventBusProvider.class, declared, component);
                case "inbox" -> exists(InboxWriterProvider.class, declared, component);
                case "dataLake" -> exists(DataLakeWriterProvider.class, declared, component);
                case "quarantineRepository" -> exists(QuarantineRepositoryProvider.class, declared, component);
                default -> throw new AssertionError("unknown component in the infrastructure table: " + component);
            }
        }
    }

    private static String asDescriptor(String yaml) {
        if (yaml.startsWith("- ")) {
            return "infrastructure:\n  eventBus: {type: IN_MEMORY}\nmodules:\n" + yaml.indent(2);
        }
        return Pattern.compile("^modules:", Pattern.MULTILINE).matcher(yaml).find() ? yaml : yaml + "modules: []\n";
    }

    private SpoolNodeDescriptor load(String descriptor) throws IOException {
        Path file = write("Spool.yaml", descriptor);
        DescriptorLoader loader = DescriptorLoader.fromFileSystem(
                name -> name.equals(DescriptorLoader.PROFILE_VARIABLE) ? null : EXAMPLE_VALUE);
        return loader.load(file.toAbsolutePath().normalize().toString());
    }

    private Path write(String name, String content) throws IOException {
        Path file = folder.resolve(name);
        Files.createDirectories(file.getParent());
        return Files.writeString(file, content);
    }

    private static void assertPluginsExist(SpoolNodeDescriptor node) {
        InfrastructureDescriptor infrastructure = node.infrastructure();
        exists(EventBusProvider.class, infrastructure.eventBus(), "infrastructure.eventBus");
        exists(InboxWriterProvider.class, infrastructure.inbox(), "infrastructure.inbox");
        exists(DataLakeWriterProvider.class, infrastructure.dataLake(), "infrastructure.dataLake");
        exists(QuarantineRepositoryProvider.class, infrastructure.quarantineRepository(), "infrastructure.quarantineRepository");
        node.authentication().forEach((name, provider) ->
                KnownPlugins.get(RequestAuthenticatorProvider.class, provider.type(), "authentication." + name, "authentication"));
        node.modules().stream().filter(CrawlerDescriptor.class::isInstance).map(CrawlerDescriptor.class::cast)
                .forEach(ReadmeExamplesTest::crawlerPluginsExist);
    }

    private static void crawlerPluginsExist(CrawlerDescriptor crawler) {
        if (crawler.type().equals("STREAM")) {
            KnownPlugins.get(StreamSourceProvider.class, crawler.source().type(), "source.type", "stream source");
        } else {
            KnownPlugins.get(PollSourceProvider.class, crawler.source().type(), "source.type", "poll source");
        }
        if (crawler.source().normalizer() instanceof Extended extended) {
            KnownPlugins.get(NormalizerProvider.class, extended.from().name(), "_NORMALIZER", extended.from().path(), "normalizer");
        }
        if (crawler.errorRouter() != null) {
            KnownPlugins.get(ErrorRouterProvider.class, crawler.errorRouter().type(), "errorRouter.type", "error router");
        }
    }

    private static <T extends Plugin<R>, R> void exists(Class<T> type, InfrastructureComponentDescriptor component,
                                                        String path) {
        if (component != null) KnownPlugins.get(type, component.type(), path, path);
    }
}
