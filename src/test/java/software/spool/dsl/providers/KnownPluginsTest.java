package software.spool.dsl.providers;

import org.junit.jupiter.api.Test;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.serde.NormalizerProvider;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KnownPluginsTest {

    @Test
    void get_aRegisteredName_returnsThePlugin() {
        assertThat(KnownPlugins.get(EventBusProvider.class, "IN_MEMORY", "infrastructure.eventBus.type", "event bus"))
            .isNotNull();
    }

    @Test
    void get_ignoresTheCaseOfTheName() {
        assertThat(KnownPlugins.get(EventBusProvider.class, "in_memory", "infrastructure.eventBus.type", "event bus"))
            .isNotNull();
    }

    @Test
    void get_anUnknownName_saysWhereItIsAndWhichOnesExist() {
        assertThatThrownBy(() ->
                KnownPlugins.get(EventBusProvider.class, "KAFKAA", "infrastructure.eventBus.type", "event bus"))
            .isInstanceOf(InvalidDescriptorException.class)
            .hasMessageStartingWith("infrastructure.eventBus.type 'KAFKAA' is not a known event bus. Known: ")
            .hasMessageContaining("IN_MEMORY")
            .hasMessageContaining("KAFKA");
    }

    @Test
    void get_anUnknownName_listsTheKnownOnesSorted() {
        String message = catchInvalidDescriptor(() ->
                KnownPlugins.get(EventBusProvider.class, "NOPE", "infrastructure.eventBus.type", "event bus"));

        List<String> known = Arrays.asList(message.substring(message.indexOf("Known: ") + 7).split(", "));

        assertThat(known).isSorted().contains("IN_MEMORY", "KAFKA");
    }

    @Test
    void get_withASuffix_findsThePluginByTheValueAlone() {
        assertThat(KnownPlugins.get(NormalizerProvider.class, "json_array", "_NORMALIZER",
                "source.mediaType", "media type")).isNotNull();
    }

    @Test
    void get_withASuffix_speaksOfTheValueAndNotOfTheSuffix() {
        String message = catchInvalidDescriptor(() ->
                KnownPlugins.get(NormalizerProvider.class, "PNG", "_NORMALIZER", "source.mediaType", "media type"));

        assertThat(message)
            .startsWith("source.mediaType 'PNG' is not a known media type. Known: ")
            .contains("JSON_ARRAY", "PDF")
            .doesNotContain("_NORMALIZER");
    }

    private static String catchInvalidDescriptor(Runnable action) {
        try {
            action.run();
        } catch (InvalidDescriptorException e) {
            return e.getMessage();
        }
        throw new AssertionError("expected an InvalidDescriptorException");
    }
}
