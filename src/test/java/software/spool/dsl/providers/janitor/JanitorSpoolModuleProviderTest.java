package software.spool.dsl.providers.janitor;

import software.spool.core.model.quarantine.Resolver;
import software.spool.dsl.Pieces;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.spool.core.model.EnvelopeStatus;
import software.spool.core.model.quarantine.QuarantineLocation;
import software.spool.core.model.quarantine.QuarantineReason;
import software.spool.core.model.quarantine.QuarantinedRecord;
import software.spool.core.model.vo.Envelope;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.EventMetadataKey;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.media.MediaTypes;
import software.spool.infrastructure.adapter.quarantine.FileSystemQuarantineRepository;
import software.spool.janitor.api.Janitor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.core.model.schedule.Schedule;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.mounter.MountReadDescriptor;
import software.spool.dsl.descriptors.module.mounter.MountWriteDescriptor;
import software.spool.dsl.descriptors.module.mounter.MounterDescriptor;
import software.spool.dsl.descriptors.module.mounter.MounterType;
import software.spool.dsl.descriptors.module.mounter.PartitioningDescriptor;
import software.spool.dsl.descriptors.module.mounter.PartitioningType;
import software.spool.dsl.providers.mounter.QuarantinedResultWriters;
import software.spool.dsl.registry.ModuleProviderRegistry;
import software.spool.mounter.api.MountMode;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JanitorSpoolModuleProviderTest {

    private final JanitorSpoolModuleProvider provider = new JanitorSpoolModuleProvider();

    private static final InfrastructureDescriptor INFRASTRUCTURE = Pieces.infrastructure(
        Pieces.eventBus("IN_MEMORY"),
        Pieces.inbox("FILE_SYSTEM", Map.of("path", "target/spool-test/inbox")),
        Pieces.quarantineRepository("FILE_SYSTEM", Map.of("path", "target/spool-test/quarantine")));

    @Test
    void create_withEveryTime_buildsTheModule() {
        JanitorDescriptor janitor = new JanitorDescriptor("j1", Optional.of(Schedule.every(Duration.ofSeconds(1))),
            Optional.of(Duration.ofSeconds(10)), Optional.of(Duration.ofDays(1)), Optional.of(Duration.ofDays(7)),
            Optional.of(Duration.ofDays(90)), Optional.of(5), Optional.of(Duration.ofHours(1)), Optional.empty());

        assertThat(provider.create(configurationOf(janitor))).isNotNull();
    }

    @Test
    void create_withDefaults_buildsTheModule() {
        assertThat(provider.create(configurationOf(JanitorDescriptor.withDefaults("j1")))).isNotNull();
    }

    @Test
    void create_withATtlLongerThanTheJanitorAccepts_namesTheKey() {
        JanitorDescriptor janitor = new JanitorDescriptor("j1", Optional.empty(), Optional.empty(),
            Optional.of(Duration.ofDays(30)), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> provider.create(configurationOf(janitor)))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("configuration.ttl must be at most ");
    }

    @Test
    void create_withADataLake_givesAJanitorThatWritesBronzeQuarantineBackToIt(@TempDir Path tmp) throws Exception {
        String quarantinePath = tmp.resolve("quarantine").toString();
        InfrastructureDescriptor infrastructure = Pieces.infrastructure(
            Pieces.eventBus("IN_MEMORY"),
            Pieces.inbox("FILE_SYSTEM", Map.of("path", tmp.resolve("inbox").toString())),
            Pieces.dataLake("FILE_SYSTEM", Map.of("path", tmp.resolve("lake").toString())),
            Pieces.quarantineRepository("FILE_SYSTEM", Map.of("path", quarantinePath)));
        PartitionKey partition = PartitionKey.ofEntriesWithoutDate().with("source", "boe").build();
        Envelope envelope = new Envelope(IdempotencyKey.of("k1"), new EventMetadata(), MediaTypes.JSON, "{}".getBytes(),
            EnvelopeStatus.PERSISTED, 0, Instant.now(), Instant.now());
        new FileSystemQuarantineRepository(quarantinePath).save(QuarantinedRecord.pending(envelope, QuarantineLocation.bronze(partition),
            QuarantineReason.VALIDATION_FAILED, List.of(), Instant.now()));

        Janitor janitor = (Janitor) provider.create(PluginConfiguration.builder()
            .with("descriptor", JanitorDescriptor.withDefaults("j1"))
            .with("infrastructure", infrastructure)
            .build());

        assertThat(janitor.quarantinedEnvelopes().republish(List.of(IdempotencyKey.of("k1")), Resolver.identified("ana", "people"))).containsExactly(IdempotencyKey.of("k1"));
        assertThat(tmp.resolve("lake/bronze/source=boe/k1.json")).exists();
    }

    @Test
    void create_withTheResultWritersOfItsNode_givesAJanitorThatWritesQuarantinedResultsToTheirDataMart(@TempDir Path tmp) throws Exception {
        String quarantinePath = tmp.resolve("quarantine").toString();
        InfrastructureComponentDescriptor[] pieces = {
            Pieces.eventBus("IN_MEMORY"),
            Pieces.inbox("FILE_SYSTEM", Map.of("path", tmp.resolve("inbox").toString())),
            Pieces.dataMart("FILE_SYSTEM", Map.of("path", tmp.resolve("mart").toString())),
            Pieces.quarantineRepository("FILE_SYSTEM", Map.of("path", quarantinePath))};
        MounterDescriptor gold = new MounterDescriptor("payments-gold", MounterType.POLL, Optional.empty(), MountMode.AGGREGATION,
            false, Optional.empty(), new MountReadDescriptor(List.of(PartitionKey.ofEntriesWithoutDate().with("source", "payments").build())),
            new MountWriteDescriptor("payments", new PartitioningDescriptor(PartitioningType.ATTRIBUTES, List.of("country"))));
        SpoolNodeDescriptor node = Pieces.node(List.of(gold), pieces);
        byte[] result = "{\"day\":\"2026-11-21\",\"country\":\"FR\",\"amount\":7}".getBytes(StandardCharsets.UTF_8);
        IdempotencyKey key = IdempotencyKey.of("gold/payments", result);
        Envelope envelope = new Envelope(key, new EventMetadata().set(EventMetadataKey.SOURCE, "gold/payments"), MediaTypes.JSON,
            result, EnvelopeStatus.QUARANTINED, 0, Instant.now(), Instant.now());
        new FileSystemQuarantineRepository(quarantinePath).save(QuarantinedRecord.pending(envelope,
            QuarantineLocation.dataMart("gold/payments", PartitionKey.ofEntriesWithoutDate().with("day", "2026-11-21").build()),
            QuarantineReason.MISSING_PARTITION_ATTRIBUTE, List.of(), Instant.now()));

        Janitor janitor = (Janitor) provider.create(PluginConfiguration.builder()
            .with("descriptor", JanitorDescriptor.withDefaults("j1"))
            .with("infrastructure", Pieces.infrastructure(pieces))
            .with(ModuleProviderRegistry.QUARANTINED_RESULT_WRITERS, QuarantinedResultWriters.of(node))
            .build());

        assertThat(janitor.quarantinedEnvelopes().republish(List.of(key), Resolver.identified("ana", "people"))).containsExactly(key);
        try (Stream<Path> written = Files.list(tmp.resolve("mart/gold/payments/country=FR"))) {
            assertThat(written).hasSize(1);
        }
    }

    private static PluginConfiguration configurationOf(JanitorDescriptor janitor) {
        return PluginConfiguration.builder()
            .with("descriptor", janitor)
            .with("infrastructure", INFRASTRUCTURE)
            .build();
    }
}
