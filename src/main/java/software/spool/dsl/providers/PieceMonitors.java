package software.spool.dsl.providers;

import software.spool.core.port.health.HealthProbe;
import software.spool.core.port.health.PieceCheckPolicy;
import software.spool.core.port.health.PieceMonitor;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.reader.ReadableDuration;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.health.HealthProbeProvider;

import java.time.Duration;
import java.util.Optional;

/**
 * Builds the monitor of a piece a module uses: the check of its type, when the plugin of that type has one, and how
 * often to check it, which the piece can set in its configuration.
 */
final class PieceMonitors {
    static final String CHECK_INTERVAL = "healthCheckInterval";
    static final String SLOW_AFTER = "healthSlowAfter";

    private PieceMonitors() {
    }

    static PieceMonitor of(InfrastructureComponentDescriptor piece) {
        return PieceMonitor.of(piece.name(), piece.pluginName(), checkOf(piece), policyOf(piece), piece.circuit());
    }

    /** The monitor of the source of a crawler, named by its id, which has no check of its own. */
    static PieceMonitor ofSource(String sourceId, SourceDescriptor source) {
        return PieceMonitor.of(sourceId, source.type(), Optional.empty(), PieceCheckPolicy.defaults(), source.circuit());
    }

    private static Optional<HealthProbe> checkOf(InfrastructureComponentDescriptor piece) {
        PluginConfiguration configuration = piece.toPluginConfiguration();
        return Optional.ofNullable(PluginResolver.available(HealthProbeProvider.class).get(piece.pluginName()))
                .filter(provider -> provider.supports(configuration))
                .map(provider -> provider.create(configuration));
    }

    private static PieceCheckPolicy policyOf(InfrastructureComponentDescriptor piece) {
        return new PieceCheckPolicy(durationOf(piece, CHECK_INTERVAL, PieceCheckPolicy.DEFAULT_INTERVAL),
                durationOf(piece, SLOW_AFTER, PieceCheckPolicy.DEFAULT_SLOW_AFTER));
    }

    private static Duration durationOf(InfrastructureComponentDescriptor piece, String key, Duration byDefault) {
        return Optional.ofNullable(piece.configuration().get(key))
                .map(value -> ReadableDuration.parse(value).orElseThrow(() -> new InvalidDescriptorException(
                        piece.path() + ".configuration." + key, "is not a duration such as 10s or 2m: " + value)))
                .orElse(byDefault);
    }
}
