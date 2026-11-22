package software.spool.dsl.providers.crawler;

import software.spool.core.model.spool.SpoolModule;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.crawler.api.builder.CrawlerBuilderFactory;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Set;


@SpoolPlugin(SpoolModuleProvider.class)
public class CrawlerPollSpoolModuleProvider implements SpoolModuleProvider {

    @Override
    public String name() { return "CRAWLER_POLL"; }

    @Override
    public int priority() { return 10; }

    @Override
    public Set<InfrastructureKind> uses(SpoolModuleDescriptor descriptor) {
        return Set.of(InfrastructureKind.EVENT_BUS, InfrastructureKind.INBOX);
    }

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
        PollSource<?> pollSource = InfrastructurePluginFactory.pollSource(infrastructure, source);
        var builder = infrastructure.watchdog() != null
                ? CrawlerBuilderFactory.watchdog(infrastructure.watchdog(), crawler.id()).poll(pollSource)
                : CrawlerBuilderFactory.poll(pollSource);
        return builder
                .source()
                    .schedule(PollingConfiguration.of(crawler.schedule()))
                    .ports(InfrastructurePluginFactory.crawlerPorts(infrastructure))
                    .enrichRules(source.enrichment())
                    .rootPath(source.rootPath())
                    .mediaType(source.mediaType())
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

}
