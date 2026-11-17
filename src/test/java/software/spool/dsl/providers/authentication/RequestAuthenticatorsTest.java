package software.spool.dsl.providers.authentication;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.authentication.AuthenticationDescriptor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestAuthenticatorsTest {

    private final RequestAuthenticators authentication = RequestAuthenticators.from(Map.of(
            "people", new AuthenticationDescriptor("OIDC", Map.of("issuer", "https://identity.example", "audience", "spool")),
            "boe-webhook", new AuthenticationDescriptor("HMAC_SIGNATURE", Map.of("header", "X-Signature-256", "secret", "s"))));

    @Test
    void find_aDeclaredProvider_givesItsAuthenticator() {
        assertThat(authentication.find("people", "modules[0].janitor.quarantineServer.authentication")).isNotNull();
    }

    @Test
    void find_aProviderThatIsNotDeclared_failsWithThePathOfTheEntryAndTheDeclaredNames() {
        assertThatThrownBy(() -> authentication.find("staff", "modules[0].janitor.quarantineServer.authentication"))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("modules[0].janitor.quarantineServer.authentication")
                .hasMessageContaining("'staff'")
                .hasMessageContaining("boe-webhook, people");
    }

    @Test
    void find_withNoProvidersDeclared_saysSo() {
        assertThatThrownBy(() -> RequestAuthenticators.none().find("people", "entry"))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("Declared: none");
    }
}
