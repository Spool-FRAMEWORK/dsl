package software.spool.dsl.providers;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfrastructurePluginFactoryTest {

    private static final InfrastructureComponentDescriptor BUS = new InfrastructureComponentDescriptor("IN_MEMORY", Map.of());

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
}
