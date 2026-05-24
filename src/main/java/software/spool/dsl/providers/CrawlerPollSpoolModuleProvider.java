package software.spool.dsl.providers;

import software.spool.core.model.Event;
import software.spool.core.model.spool.SpoolModule;
import software.spool.core.utils.media.MediaTypeResolver;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.crawler.api.builder.CrawlerBuilderFactory;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.crawler.internal.utils.factory.Normalizer;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.PollSourceProvider;

import java.time.Duration;
import java.util.List;

@SpoolPlugin(SpoolModuleProvider.class)
public class CrawlerPollSpoolModuleProvider implements SpoolModuleProvider {

    @Override
    public String name() { return "CRAWLER_POLL"; }

    @Override
    public int priority() { return 10; }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.get("descriptor", SpoolModuleDescriptor.class)
                .filter(d -> d instanceof CrawlerDescriptor c && c.type().equals("POLL"))
                .isPresent();
    }

    @Override
    public SpoolModule create(PluginConfiguration configuration) {
        CrawlerDescriptor crawler = configuration.require("descriptor", CrawlerDescriptor.class);
        InfrastructureDescriptor infrastructure = configuration.require("infrastructure", InfrastructureDescriptor.class);
        SourceDescriptor source = crawler.source();
        PollSource<?> pollSource = PluginResolver.resolve(PollSourceProvider.class,
                PluginConfiguration.of(source.configuration()));
        var builder = infrastructure.watchdog() != null
                ? CrawlerBuilderFactory.watchdog(infrastructure.watchdog(), crawler.id()).poll(pollSource)
                : CrawlerBuilderFactory.poll(pollSource);
        return builder
                .source()
                    .schedule(buildSchedule(source))
                    .ports(InfrastructurePluginFactory.crawlerPorts(infrastructure))
                    .enrichRules(source.enrichment())
                    .rootPath(source.rootPath())
                    .mediaType(MediaTypeResolver.resolve(source.mediaType()))
                    .and()
                .mapping()
                    .convention(crawler.eventMapping().namingConvention())
                    .addDomainEvent(resolveEventClasses(crawler.eventMapping().domainMappingList()), crawler.eventMapping().attributeList().toArray(String[]::new))
                    .addPartitionAttributes(crawler.eventMapping().attributeList().toArray(String[]::new))
                    .and()
                .createWith(objectNormalizer(source));
    }

    private List<Class<? extends Event>> resolveEventClasses(List<String> names) {
        return names.stream()
                .map(this::toClass)
                .map(c -> (Class<? extends Event>) c)
                .collect(java.util.stream.Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Event> toClass(String name) {
        try {
            return (Class<? extends Event>) Class.forName("events." + name);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to resolve event class: " + name, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <I> Normalizer<I> objectNormalizer(SourceDescriptor source) {
        return (Normalizer<I>) InfrastructurePluginFactory.normalizer(source);
    }

    private PollingConfiguration buildSchedule(SourceDescriptor source) {
        String every = source.configuration().get("scheduleMilliseconds");
        if (every == null || Integer.parseInt(every) == 0) return PollingConfiguration.once();
        return PollingConfiguration.every(Duration.ofMillis(Integer.parseInt(every)));
    }
}
