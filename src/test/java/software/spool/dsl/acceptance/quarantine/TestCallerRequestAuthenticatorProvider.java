package software.spool.dsl.acceptance.quarantine;

import software.spool.core.port.authentication.Authentication;
import software.spool.core.port.authentication.Identity;
import software.spool.core.port.authentication.RequestAuthenticator;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.authentication.RequestAuthenticatorProvider;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An authentication provider for the scenarios only: the caller says who they are in a header, and gets the roles of
 * the provider's configuration. A request without the header is not identified.
 */
@SpoolPlugin(RequestAuthenticatorProvider.class)
public class TestCallerRequestAuthenticatorProvider implements RequestAuthenticatorProvider {
    public static final String TYPE = "TEST_CALLER";
    public static final String CALLER_HEADER = "X-Test-Caller";
    private static final String ROLES = "roles";
    private static final String ROLE_SEPARATOR = ",";

    @Override
    public String name() {
        return TYPE;
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return true;
    }

    @Override
    public RequestAuthenticator create(PluginConfiguration configuration) {
        String provider = configuration.require(NAME, String.class);
        Set<String> roles = Arrays.stream(configuration.get(ROLES).orElse("").split(ROLE_SEPARATOR))
                .map(String::strip)
                .filter(role -> !role.isEmpty())
                .collect(Collectors.toSet());
        return request -> request.header(CALLER_HEADER)
                .map(caller -> Authentication.accepted(new Identity(caller, roles, provider)))
                .orElseGet(() -> Authentication.rejected("the request has no " + CALLER_HEADER + " header"));
    }
}
