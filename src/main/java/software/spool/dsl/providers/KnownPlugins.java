package software.spool.dsl.providers;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.infrastructure.PluginRegistry;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.Plugin;

import java.util.stream.Collectors;

/**
 * Looks up the plugin a descriptor names. When there is none the error says where the name is in the
 * descriptor and which ones exist, and the list comes from what is registered, so a new plugin shows up
 * in it without changing anything here.
 */
public final class KnownPlugins {

    private KnownPlugins() {}

    public static <T extends Plugin<R>, R> T get(Class<T> type, String name, String path, String description) {
        return get(type, name, "", path, description);
    }

    /**
     * For plugins registered as {@code <value><suffix>}, like the normalizers: the error speaks of the
     * value alone, which is what the descriptor holds.
     */
    public static <T extends Plugin<R>, R> T get(Class<T> type, String value, String suffix, String path, String description) {
        String name = value.toUpperCase() + suffix;
        try {
            return PluginResolver.get(type, name);
        } catch (IllegalStateException e) {
            if (PluginRegistry.find(type, name).isPresent()) throw e;
            throw new InvalidDescriptorException(path,
                    "'" + value + "' is not a known " + description + ". Known: " + known(type, suffix));
        }
    }

    private static <T extends Plugin<R>, R> String known(Class<T> type, String suffix) {
        return PluginRegistry.findAll(type).keySet().stream()
                .map(name -> name.endsWith(suffix) ? name.substring(0, name.length() - suffix.length()) : name)
                .sorted()
                .collect(Collectors.joining(", "));
    }
}
