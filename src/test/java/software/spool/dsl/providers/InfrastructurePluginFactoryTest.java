package software.spool.dsl.providers;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfrastructurePluginFactoryTest {

    private static final InfrastructureComponentDescriptor BUS = new InfrastructureComponentDescriptor("IN_MEMORY", Map.of());

    private static SourceDescriptor source(String mediaType) {
        return new SourceDescriptor("HTTP", "s", Map.of(), mediaType, "", List.of());
    }

    @Test
    void eventBus_needsNoOtherComponent() {
        InfrastructureDescriptor onlyTheBus = new InfrastructureDescriptor(null, BUS, null, null);

        assertThat(InfrastructurePluginFactory.eventBus(onlyTheBus)).isNotNull();
    }

    @Test
    void eventBus_withoutIt_namesTheKey() {
        InfrastructureDescriptor none = new InfrastructureDescriptor(null, null, null, null);

        assertThatThrownBy(() -> InfrastructurePluginFactory.eventBus(none))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessage("infrastructure.eventBus is required");
    }

    @Test
    void eventBus_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = new InfrastructureDescriptor(null,
            new InfrastructureComponentDescriptor("KAFKAA", Map.of()), null, null);

        assertThatThrownBy(() -> InfrastructurePluginFactory.eventBus(unknown))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("infrastructure.eventBus.type 'KAFKAA' is not a known event bus. Known: ")
            .hasMessageContaining("IN_MEMORY");
    }

    @Test
    void inboxWriter_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = new InfrastructureDescriptor(null, BUS,
            new InfrastructureComponentDescriptor("NOPE", Map.of()), null);

        assertThatThrownBy(() -> InfrastructurePluginFactory.inboxWriter(unknown))
            .hasMessageStartingWith("infrastructure.inbox.type 'NOPE' is not a known inbox writer. Known: ")
            .hasMessageContaining("FILE_SYSTEM");
    }

    @Test
    void dataLakeWriter_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = new InfrastructureDescriptor(null, BUS, BUS,
            new InfrastructureComponentDescriptor("NOPE", Map.of()));

        assertThatThrownBy(() -> InfrastructurePluginFactory.dataLakeWriter(unknown))
            .hasMessageStartingWith("infrastructure.dataLake.type 'NOPE' is not a known data lake writer. Known: ")
            .hasMessageContaining("FILE_SYSTEM");
    }

    @Test
    void normalizer_withAKnownMediaType_returnsIt() {
        assertThat(InfrastructurePluginFactory.normalizer(source("JSON_ARRAY"))).isNotNull();
    }

    @Test
    void normalizer_withAnUnknownMediaType_namesTheKeyAndTheKnownOnes() {
        assertThatThrownBy(() -> InfrastructurePluginFactory.normalizer(source("PNG")))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("source.mediaType 'PNG' is not a known media type. Known: ")
            .hasMessageContaining("JSON_ARRAY")
            .hasMessageNotContaining("_NORMALIZER");
    }

    @Test
    void inboxWriter_withoutInbox_namesTheKey() {
        InfrastructureDescriptor withoutInbox = new InfrastructureDescriptor(null, BUS, null, null);

        assertThatThrownBy(() -> InfrastructurePluginFactory.inboxWriter(withoutInbox))
            .hasMessage("infrastructure.inbox is required");
    }

    @Test
    void dataLakeWriter_withoutDataLake_namesTheKey() {
        InfrastructureDescriptor withoutDataLake = new InfrastructureDescriptor(null, BUS, BUS, null);

        assertThatThrownBy(() -> InfrastructurePluginFactory.dataLakeWriter(withoutDataLake))
            .hasMessage("infrastructure.dataLake is required");
    }

    @Test
    void quarantineStore_withoutIt_namesTheKey() {
        InfrastructureDescriptor withoutQuarantineStore = new InfrastructureDescriptor(null, BUS, BUS, BUS);

        assertThatThrownBy(() -> InfrastructurePluginFactory.quarantineStore(withoutQuarantineStore))
            .hasMessage("infrastructure.quarantineStore is required");
    }

    @Test
    void quarantineStore_withAnUnknownType_namesTheKeyAndTheKnownOnes() {
        InfrastructureDescriptor unknown = new InfrastructureDescriptor(null, BUS, BUS, BUS,
            new InfrastructureComponentDescriptor("NOPE", Map.of()));

        assertThatThrownBy(() -> InfrastructurePluginFactory.quarantineStore(unknown))
            .hasMessageStartingWith("infrastructure.quarantineStore.type 'NOPE' is not a known quarantine store. Known: ")
            .hasMessageContaining("FILE_SYSTEM");
    }

    @Test
    void quarantineStore_withAKnownType_returnsIt() {
        InfrastructureDescriptor withQuarantineStore = new InfrastructureDescriptor(null, BUS, BUS, BUS,
            new InfrastructureComponentDescriptor("FILE_SYSTEM", Map.of("path", "target/spool-test/quarantine")));

        assertThat(InfrastructurePluginFactory.quarantineStore(withQuarantineStore)).isNotNull();
    }
}
