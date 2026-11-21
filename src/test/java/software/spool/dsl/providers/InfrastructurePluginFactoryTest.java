package software.spool.dsl.providers;

import org.junit.jupiter.api.Test;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.core.utils.media.MediaTypes;
import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.Pieces;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor.Extended;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfrastructurePluginFactoryTest {

    private static final InfrastructureComponentDescriptor BUS = Pieces.eventBus("IN_MEMORY");
    private static final InfrastructureComponentDescriptor INBOX = Pieces.inbox("FILE_SYSTEM", Map.of("path", "target/spool-test/inbox"));
    private static final InfrastructureComponentDescriptor LAKE = Pieces.dataLake("FILE_SYSTEM", Map.of("path", "target/spool-test/lake"));

    private static SourceDescriptor source(String normalizer) {
        return new SourceDescriptor("HTTP", "s", Map.of(), Extended.named(normalizer, "source.normalizer"),
            MediaTypes.JSON, "", List.of());
    }

    @SuppressWarnings("unchecked")
    private static Normalizer<byte[]> bytesNormalizer(SourceDescriptor source) {
        return (Normalizer<byte[]>) InfrastructurePluginFactory.normalizer(source);
    }

    @Test
    void eventBus_needsNoOtherComponent() {
        InfrastructureDescriptor onlyTheBus = Pieces.infrastructure(BUS);

        assertThat(InfrastructurePluginFactory.eventBus(onlyTheBus)).isNotNull();
    }

    @Test
    void eventBus_withoutIt_namesTheKind() {
        InfrastructureDescriptor none = Pieces.infrastructure();

        assertThatThrownBy(() -> InfrastructurePluginFactory.eventBus(none))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessage("the module needs an EVENT_BUS, and infrastructure declares none");
    }

    @Test
    void eventBus_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = Pieces.infrastructure(Pieces.eventBus("KAFKAA"));

        assertThatThrownBy(() -> InfrastructurePluginFactory.eventBus(unknown))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("infrastructure.eventBus.type 'KAFKAA' is not a known event bus. Known: ")
            .hasMessageContaining("IN_MEMORY");
    }

    @Test
    void inboxWriter_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = Pieces.infrastructure(BUS, Pieces.inbox("NOPE", Map.of()));

        assertThatThrownBy(() -> InfrastructurePluginFactory.inboxWriter(unknown))
            .hasMessageStartingWith("infrastructure.inbox.type 'NOPE' is not a known inbox writer. Known: ")
            .hasMessageContaining("FILE_SYSTEM");
    }

    @Test
    void dataLakeWriter_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = Pieces.infrastructure(BUS, INBOX, Pieces.dataLake("NOPE", Map.of()));

        assertThatThrownBy(() -> InfrastructurePluginFactory.dataLakeWriter(unknown))
            .hasMessageStartingWith("infrastructure.dataLake.type 'NOPE' is not a known data lake writer. Known: ")
            .hasMessageContaining("FILE_SYSTEM");
    }

    @Test
    void normalizer_withAKnownName_returnsIt() {
        assertThat(InfrastructurePluginFactory.normalizer(source("JSON_ARRAY"))).isNotNull();
    }

    @Test
    void normalizer_withAJsonObject_turnsOneObjectIntoOneRecord() {
        Normalizer<byte[]> normalizer = bytesNormalizer(source("JSON_OBJECT"));

        List<String> records = normalizer.normalize("{\"id\":1}".getBytes(StandardCharsets.UTF_8))
            .map(record -> new String(record, StandardCharsets.UTF_8))
            .toList();

        assertThat(records).containsExactly("{\"id\":1}");
    }

    @Test
    void normalizer_withAJsonObjectAndARootPathAndARule_keepsTheObjectAndAddsTheField() {
        SourceDescriptor source = new SourceDescriptor("HTTP", "s", Map.of(),
            Extended.named("JSON_OBJECT", "source.normalizer"), MediaTypes.JSON, "data",
            List.of(new EnrichmentRule("meta.origin", "origin")));
        Normalizer<byte[]> normalizer = bytesNormalizer(source);

        List<String> records = normalizer.normalize("{\"data\":{\"id\":1},\"meta\":{\"origin\":\"x\"}}".getBytes(StandardCharsets.UTF_8))
            .map(record -> new String(record, StandardCharsets.UTF_8))
            .toList();

        assertThat(records).containsExactly("{\"id\":1,\"origin\":\"x\"}");
    }

    @Test
    void normalizer_withAnUnknownName_namesTheKeyAndTheKnownOnes() {
        assertThatThrownBy(() -> InfrastructurePluginFactory.normalizer(source("PNG")))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("source.normalizer 'PNG' is not a known normalizer. Known: ")
            .hasMessageContaining("JSON_ARRAY")
            .hasMessageNotContaining("_NORMALIZER");
    }

    @Test
    void inboxWriter_withoutInbox_namesTheKind() {
        InfrastructureDescriptor withoutInbox = Pieces.infrastructure(BUS);

        assertThatThrownBy(() -> InfrastructurePluginFactory.inboxWriter(withoutInbox))
            .hasMessage("the module needs an INBOX, and infrastructure declares none");
    }

    @Test
    void dataLakeWriter_withoutDataLake_namesTheKind() {
        InfrastructureDescriptor withoutDataLake = Pieces.infrastructure(BUS, INBOX);

        assertThatThrownBy(() -> InfrastructurePluginFactory.dataLakeWriter(withoutDataLake))
            .hasMessage("the module needs a DATA_LAKE, and infrastructure declares none");
    }

    @Test
    void quarantineRepository_withoutIt_namesTheKind() {
        InfrastructureDescriptor withoutQuarantineRepository = Pieces.infrastructure(BUS, INBOX, LAKE);

        assertThatThrownBy(() -> InfrastructurePluginFactory.quarantineRepository(withoutQuarantineRepository))
            .hasMessage("the module needs a QUARANTINE_REPOSITORY, and infrastructure declares none");
    }

    @Test
    void quarantineRepository_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = Pieces.infrastructure(BUS, INBOX, LAKE, Pieces.quarantineRepository("NOPE", Map.of()));

        assertThatThrownBy(() -> InfrastructurePluginFactory.quarantineRepository(unknown))
            .hasMessageStartingWith("infrastructure.quarantineRepository.type 'NOPE' is not a known quarantine repository. Known: ")
            .hasMessageContaining("FILE_SYSTEM");
    }

    @Test
    void quarantineRepository_withAKnownType_returnsIt() {
        InfrastructureDescriptor withQuarantineRepository = Pieces.infrastructure(BUS, INBOX, LAKE,
            Pieces.quarantineRepository("FILE_SYSTEM", Map.of("path", "target/spool-test/quarantine")));

        assertThat(InfrastructurePluginFactory.quarantineRepository(withQuarantineRepository)).isNotNull();
    }
}
