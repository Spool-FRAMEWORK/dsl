package software.spool.dsl.providers.authentication;

import software.spool.core.port.authentication.RequestAuthenticator;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.authentication.AuthenticationDescriptor;
import software.spool.dsl.providers.KnownPlugins;
import software.spool.infrastructure.spi.provider.InvalidPluginConfigurationException;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.authentication.RequestAuthenticatorProvider;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The providers of the {@code authentication} block, built once when the descriptor is loaded (ADR-013). A provider
 * of an unknown type, or one its plugin cannot build, stops the load with the path of the wrong value. An HTTP entry
 * names its provider and gets it with {@link #find}.
 */
public final class RequestAuthenticators {
    private static final String AUTHENTICATION = "authentication";

    private final Map<String, RequestAuthenticator> byName;

    private RequestAuthenticators(Map<String, RequestAuthenticator> byName) {
        this.byName = Map.copyOf(byName);
    }

    public static RequestAuthenticators none() {
        return new RequestAuthenticators(Map.of());
    }

    public static RequestAuthenticators from(Map<String, AuthenticationDescriptor> providers) {
        Map<String, RequestAuthenticator> built = new LinkedHashMap<>();
        providers.forEach((name, provider) -> built.put(name, build(name, provider)));
        return new RequestAuthenticators(built);
    }

    /** The names of the declared providers. */
    public Set<String> names() {
        return byName.keySet();
    }

    /**
     * The provider an entry names.
     *
     * @param path where the entry names it in the descriptor, for the error
     * @throws InvalidDescriptorException if no provider has that name
     */
    public RequestAuthenticator find(String name, String path) {
        RequestAuthenticator authenticator = byName.get(name);
        if (authenticator == null) {
            throw new InvalidDescriptorException(path, "names the authentication provider '" + name
                    + "', which is not declared in " + AUTHENTICATION + ". Declared: "
                    + (byName.isEmpty() ? "none" : String.join(", ", byName.keySet().stream().sorted().toList())));
        }
        return authenticator;
    }

    private static RequestAuthenticator build(String name, AuthenticationDescriptor provider) {
        String path = AUTHENTICATION + "." + name;
        RequestAuthenticatorProvider plugin = KnownPlugins.get(RequestAuthenticatorProvider.class, provider.type(),
                path + ".type", "authentication provider");
        try {
            return plugin.create(PluginConfiguration.of(provider.configuration(), Map.of(RequestAuthenticatorProvider.NAME, name)));
        } catch (InvalidPluginConfigurationException e) {
            String key = e.key();
            throw new InvalidDescriptorException(path + ".configuration." + key, e.getMessage().substring(key.length()).trim());
        }
    }
}
