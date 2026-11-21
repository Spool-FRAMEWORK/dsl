package software.spool.dsl.providers.crawler;

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
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.KnownPlugins;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.PollSourceProvider;


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
        PollSource<?> pollSource = KnownPlugins.get(PollSourceProvider.class, source.type(), "source.type", "poll source")
                .create(PluginConfiguration.of(source.configuration()));
        var builder = infrastructure.watchdog() != null
                ? CrawlerBuilderFactory.watchdog(infrastructure.watchdog(), crawler.id()).poll(pollSource)
                : CrawlerBuilderFactory.poll(pollSource);
        return builder
                .source()
                    .schedule(scheduleOf(crawler))
                    .ports(InfrastructurePluginFactory.crawlerPorts(infrastructure))
                    .enrichRules(source.enrichment())
                    .rootPath(source.rootPath())
                    .mediaType(MediaTypeResolver.resolve(source.mediaType()))
                    .and()
                .mapping()
                    .convention(crawler.eventMapping().namingConvention())
                    .addDomainEvent(crawler.eventMapping().domainEvents(), crawler.eventMapping().attributeList().toArray(String[]::new))
                    .addPartitionAttributes(crawler.eventMapping().attributeList().toArray(String[]::new))
                    .and()
                .observability()
                    .withErrorRouter(CrawlerErrorRouters.resolve(crawler))
                    .and()
                .createWith(objectNormalizer(source));
    }

    @SuppressWarnings("unchecked")
    private static <I> Normalizer<I> objectNormalizer(SourceDescriptor source) {
        return (Normalizer<I>) InfrastructurePluginFactory.normalizer(source);
    }

    private static PollingConfiguration scheduleOf(CrawlerDescriptor crawler) {
        return crawler.schedule()
                .map(schedule -> PollingConfiguration.every(schedule.every()))
                .orElseGet(PollingConfiguration::once);
    }
}
