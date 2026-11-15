package software.spool.dsl.providers.janitor;

import org.junit.jupiter.api.Test;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JanitorSpoolModuleProviderTest {

    private final JanitorSpoolModuleProvider provider = new JanitorSpoolModuleProvider();

    private static final InfrastructureDescriptor INFRASTRUCTURE = new InfrastructureDescriptor(
        null,
        new InfrastructureComponentDescriptor("IN_MEMORY", Map.of()),
        new InfrastructureComponentDescriptor("FILE_SYSTEM", Map.of("path", "target/spool-test/inbox")),
        null,
        new InfrastructureComponentDescriptor("FILE_SYSTEM", Map.of("path", "target/spool-test/quarantine")));

    @Test
    void buildQuarantineTtl_withTheKey_returnsItAsAnInteger() {
        JanitorDescriptor janitor = new JanitorDescriptor("j1", Map.of("millisecondsQuarantineTTL", "604800000"));

        assertThat(provider.buildQuarantineTtl(janitor)).isEqualTo(604_800_000);
    }

    @Test
    void buildQuarantineTtl_withoutTheKey_isNullSoNothingIsDeleted() {
        assertThat(provider.buildQuarantineTtl(new JanitorDescriptor("j1", Map.of()))).isNull();
    }

    @Test
    void buildQuarantineTtl_isIndependentOfTheOtherTtl() {
        JanitorDescriptor janitor = new JanitorDescriptor("j1", Map.of("millisecondsTTL", "86400000"));

        assertThat(provider.buildQuarantineTtl(janitor)).isNull();
    }

    @Test
    void create_withAQuarantineTtl_buildsTheModule() {
        JanitorDescriptor janitor = new JanitorDescriptor("j1", Map.of("millisecondsQuarantineTTL", "604800000"));

        assertThat(provider.create(configurationOf(janitor))).isNotNull();
    }

    @Test
    void create_withoutAQuarantineTtl_buildsTheModule() {
        assertThat(provider.create(configurationOf(new JanitorDescriptor("j1", Map.of())))).isNotNull();
    }

    @Test
    void buildQuarantineRetention_withTheKey_returnsItAsALongBeyondTheIntegerRange() {
        JanitorDescriptor janitor = new JanitorDescriptor("j1", Map.of("millisecondsQuarantineRetention", "7776000000"));

        assertThat(provider.buildQuarantineRetention(janitor)).isEqualTo(7_776_000_000L);
    }

    @Test
    void buildQuarantineRetention_withoutTheKey_isNullSoResolvedRecordsAreKept() {
        assertThat(provider.buildQuarantineRetention(new JanitorDescriptor("j1", Map.of()))).isNull();
    }

    @Test
    void create_withAQuarantineRetention_buildsTheModule() {
        JanitorDescriptor janitor = new JanitorDescriptor("j1", Map.of("millisecondsQuarantineRetention", "7776000000"));

        assertThat(provider.create(configurationOf(janitor))).isNotNull();
    }

    private static PluginConfiguration configurationOf(JanitorDescriptor janitor) {
        return PluginConfiguration.builder()
            .with("descriptor", janitor)
            .with("infrastructure", INFRASTRUCTURE)
            .build();
    }
}
