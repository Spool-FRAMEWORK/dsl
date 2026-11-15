package software.spool.dsl.providers.janitor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.infrastructure.adapter.quarantine.FileSystemQuarantineRepository;
import software.spool.janitor.api.Janitor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
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

    @Test
    void create_withADataLake_givesAJanitorThatWritesBronzeQuarantineBackToIt(@TempDir Path tmp) throws Exception {
        String quarantinePath = tmp.resolve("quarantine").toString();
        InfrastructureDescriptor infrastructure = new InfrastructureDescriptor(
            null,
            new InfrastructureComponentDescriptor("IN_MEMORY", Map.of()),
            new InfrastructureComponentDescriptor("FILE_SYSTEM", Map.of("path", tmp.resolve("inbox").toString())),
            new InfrastructureComponentDescriptor("FILE_SYSTEM", Map.of("path", tmp.resolve("lake").toString())),
            new InfrastructureComponentDescriptor("FILE_SYSTEM", Map.of("path", quarantinePath)));
        PartitionKey partition = PartitionKey.ofEntriesWithoutDate().with("source", "boe").build();
        Envelope envelope = new Envelope(IdempotencyKey.of("k1"), new EventMetadata(), MediaTypes.JSON, "{}".getBytes(),
            EnvelopeStatus.PERSISTED, 0, Instant.now(), Instant.now());
        new FileSystemQuarantineRepository(quarantinePath).save(QuarantinedRecord.pending(envelope, QuarantineLocation.bronze(partition),
            QuarantineReason.VALIDATION_FAILED, List.of(), Instant.now()));

        Janitor janitor = (Janitor) provider.create(PluginConfiguration.builder()
            .with("descriptor", new JanitorDescriptor("j1", Map.of()))
            .with("infrastructure", infrastructure)
            .build());

        assertThat(janitor.quarantinedEnvelopes().republish(List.of(IdempotencyKey.of("k1")))).containsExactly(IdempotencyKey.of("k1"));
        assertThat(tmp.resolve("lake/bronze/source=boe/k1.json")).exists();
    }

    private static PluginConfiguration configurationOf(JanitorDescriptor janitor) {
        return PluginConfiguration.builder()
            .with("descriptor", janitor)
            .with("infrastructure", INFRASTRUCTURE)
            .build();
    }
}
