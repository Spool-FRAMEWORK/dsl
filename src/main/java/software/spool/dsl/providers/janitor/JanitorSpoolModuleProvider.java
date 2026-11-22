package software.spool.dsl.providers.janitor;

import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.spool.SpoolModule;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.descriptors.module.janitor.QuarantineServerDescriptor;
import software.spool.dsl.providers.authentication.RequestAuthenticators;
import software.spool.dsl.providers.mounter.QuarantinedResultWriters;
import software.spool.dsl.registry.ModuleProviderRegistry;
import software.spool.janitor.api.quarantine.QuarantinePageSizes;
import software.spool.janitor.api.quarantine.QuarantineRoles;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.janitor.api.builder.JanitorBuilderFactory;
import software.spool.janitor.api.builder.PollingJanitorBuilder;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Set;
import java.time.Duration;
import java.util.List;

@SpoolPlugin(SpoolModuleProvider.class)
public class JanitorSpoolModuleProvider implements SpoolModuleProvider {
    private static final Schedule DEFAULT_ROUND_SCHEDULE = Schedule.every(Duration.ofSeconds(5));
    private static final String QUARANTINE_SERVER = "quarantineServer";
    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final int DEFAULT_MAX_PAGE_SIZE = 1000;
    private static final Duration LONGEST_INT_MILLIS = Duration.ofMillis(Integer.MAX_VALUE);

    @Override
    public String name() {
        return "JANITOR";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public Set<InfrastructureKind> uses(SpoolModuleDescriptor descriptor) {
        return Set.of(InfrastructureKind.EVENT_BUS, InfrastructureKind.INBOX, InfrastructureKind.QUARANTINE_REPOSITORY,
                InfrastructureKind.DATA_LAKE, InfrastructureKind.ENVELOPE_HEADERS);
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.get("descriptor", SpoolModuleDescriptor.class)
                .filter(d -> d instanceof JanitorDescriptor)
                .isPresent();
    }

    @Override
    public SpoolModule create(PluginConfiguration configuration) {
        JanitorDescriptor janitor = configuration.require("descriptor", JanitorDescriptor.class);
        InfrastructureDescriptor infrastructure = configuration.require("infrastructure", InfrastructureDescriptor.class);
        var bus = InfrastructurePluginFactory.eventBus(infrastructure);
        PollingJanitorBuilder builder = JanitorBuilderFactory.watchdog(infrastructure.watchdog(), janitor.id())
                .polling()
                .from(InfrastructurePluginFactory.inboxReader(infrastructure))
                .with(InfrastructurePluginFactory.inboxUpdater(infrastructure))
                .removeWith(InfrastructurePluginFactory.inboxEnvelopeRemover(infrastructure))
                .on(InfrastructurePluginFactory.tracedEventPublisher(infrastructure))
                .subscribeWith(bus)
                .quarantineIn(InfrastructurePluginFactory.quarantineRepository(infrastructure))
                .schedule(janitor.schedule().orElse(DEFAULT_ROUND_SCHEDULE));
        janitor.threshold().ifPresent(threshold -> builder.withMillisecondsThreshold(intMillis("threshold", threshold)));
        janitor.ttl().ifPresent(ttl -> builder.withMillisecondsTtl(intMillis("ttl", ttl)));
        janitor.quarantineTtl().ifPresent(ttl -> builder.withMillisecondsQuarantineTtl(intMillis("quarantineTtl", ttl)));
        janitor.quarantineRetention().ifPresent(retention -> builder.withMillisecondsQuarantineRetention(retention.toMillis()));
        janitor.maxInboxUpdateAttempts().ifPresent(builder::withMaxInboxUpdateAttempts);
        janitor.processedKeysWindow().ifPresent(builder::rememberProcessedKeysFor);
        if (infrastructure.find(InfrastructureKind.DATA_LAKE).isPresent()) {
            builder.rewriteDataLakeWith(InfrastructurePluginFactory.dataLakeWriter(infrastructure));
        }
        configuration.get(ModuleProviderRegistry.QUARANTINED_RESULT_WRITERS, QuarantinedResultWriters.class)
                .map(QuarantinedResultWriters::create).orElseGet(List::of)
                .forEach(builder::rewriteDataMartWith);
        janitor.quarantineServer().ifPresent(server -> serveQuarantine(builder, janitor.id(), server,
                configuration.get(ModuleProviderRegistry.AUTHENTICATION, RequestAuthenticators.class).orElseGet(RequestAuthenticators::none)));
        return builder.create();
    }

    private static void serveQuarantine(PollingJanitorBuilder builder, String id, QuarantineServerDescriptor server,
                                        RequestAuthenticators authentication) {
        try {
            builder.serveQuarantineOverHttp(
                    authentication.find(server.authentication(), QUARANTINE_SERVER + ".authentication"),
                    new QuarantineRoles(server.readRole(), server.resolveRole(), server.bypassRole()),
                    pageSizes(server));
        } catch (InvalidDescriptorException e) {
            throw e.inModule(id);
        }
    }

    private static QuarantinePageSizes pageSizes(QuarantineServerDescriptor server) {
        int byDefault = server.pageSize().orElse(DEFAULT_PAGE_SIZE);
        int maximum = server.maxPageSize().orElse(Math.max(DEFAULT_MAX_PAGE_SIZE, byDefault));
        if (byDefault < 1) {
            throw new InvalidDescriptorException(QUARANTINE_SERVER + ".pageSize", "must be at least 1, got " + byDefault);
        }
        if (maximum < byDefault) {
            throw new InvalidDescriptorException(QUARANTINE_SERVER + ".maxPageSize",
                    "must be at least the pageSize (" + byDefault + "), got " + maximum);
        }
        return new QuarantinePageSizes(byDefault, maximum);
    }

    /** The janitor takes some times as an int of milliseconds; a longer one fails instead of wrapping around. */
    private static int intMillis(String key, Duration duration) {
        if (duration.compareTo(LONGEST_INT_MILLIS) > 0) {
            throw new InvalidDescriptorException("configuration." + key,
                    "must be at most " + LONGEST_INT_MILLIS + ", got " + duration);
        }
        return (int) duration.toMillis();
    }
}
