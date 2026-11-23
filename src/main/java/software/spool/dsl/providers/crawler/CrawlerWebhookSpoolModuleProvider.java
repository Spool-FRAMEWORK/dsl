package software.spool.dsl.providers.crawler;

import software.spool.core.model.spool.SpoolModule;
import software.spool.crawler.api.builder.CrawlerBuilderFactory;
import software.spool.crawler.api.builder.WebhookCrawlerBuilder;
import software.spool.crawler.api.normalizer.Normalizer;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.descriptors.module.crawler.WebhookDescriptor;
import software.spool.dsl.providers.InfrastructurePluginFactory;
import software.spool.dsl.providers.SpoolModuleProvider;
import software.spool.dsl.providers.authentication.RequestAuthenticators;
import software.spool.dsl.registry.ModuleProviderRegistry;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Optional;
import java.util.Set;

/**
 * Builds a WEBHOOK crawler: it receives its records on the node's HTTP server, at the path of its source, from senders
 * the provider of its webhook block identifies and who hold its role (ADR-013). The path, the provider and the role
 * are required, so no webhook is ever open.
 */
@SpoolPlugin(SpoolModuleProvider.class)
public class CrawlerWebhookSpoolModuleProvider implements SpoolModuleProvider {
    private static final String TYPE = "WEBHOOK";
    private static final String SOURCE_ID = "sourceId";
    private static final String PATH = "path";

    @Override
    public String name() {
        return "CRAWLER_" + TYPE;
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public Set<InfrastructureKind> uses(SpoolModuleDescriptor descriptor) {
        return Set.of(InfrastructureKind.EVENT_BUS, InfrastructureKind.INBOX);
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return configuration.get("descriptor", SpoolModuleDescriptor.class)
                .filter(descriptor -> descriptor instanceof CrawlerDescriptor crawler && crawler.type().equals(TYPE))
                .isPresent();
    }

    @Override
    public SpoolModule create(PluginConfiguration configuration) {
        CrawlerDescriptor crawler = configuration.require("descriptor", CrawlerDescriptor.class);
        InfrastructureDescriptor infrastructure = configuration.require("infrastructure", InfrastructureDescriptor.class);
        RequestAuthenticators authentication = configuration.get(ModuleProviderRegistry.AUTHENTICATION, RequestAuthenticators.class)
                .orElseGet(RequestAuthenticators::none);
        try {
            return create(crawler, infrastructure, authentication);
        } catch (InvalidDescriptorException e) {
            throw e.inModule(crawler.id());
        }
    }

    private static SpoolModule create(CrawlerDescriptor crawler, InfrastructureDescriptor infrastructure,
                                      RequestAuthenticators authentication) {
        SourceDescriptor source = crawler.source();
        WebhookDescriptor webhook = crawler.webhook();
        String path = required(Optional.ofNullable(source.configuration().get(PATH)), "source.configuration." + PATH);
        String sourceId = required(Optional.ofNullable(source.configuration().get(SOURCE_ID)), "source.configuration." + SOURCE_ID);
        String providerName = required(webhook.authentication(), "webhook.authentication");
        String role = required(webhook.role(), "webhook.role");
        WebhookCrawlerBuilder builder = infrastructure.watchdog() != null
                ? CrawlerBuilderFactory.watchdog(infrastructure.watchdog(), crawler.id()).webhook(sourceId)
                : CrawlerBuilderFactory.named(crawler.id()).webhook(sourceId);
        builder.source()
                .ports(InfrastructurePluginFactory.crawlerPorts(infrastructure))
                .enrichRules(source.enrichment())
                .rootPath(source.rootPath())
                .identifyingRecordsWith(source.idempotencyKeySchema())
                .mediaType(source.mediaType())
                .receivingAt(path)
                .authenticatedBy(authentication.find(providerName, "webhook.authentication"), role);
        webhook.maxBodySize().ifPresent(bytes -> {
            if (bytes < 1) throw new InvalidDescriptorException("webhook.maxBodySize", "must be at least 1 byte, got " + bytes);
            builder.source().acceptingAtMost(bytes);
        });
        builder.mapping()
                .convention(crawler.eventMapping().namingConvention())
                .addDomainEvent(crawler.eventMapping().domainEvents(), crawler.eventMapping().attributeList().toArray(String[]::new))
                .addPartitionAttributes(crawler.eventMapping().attributeList().toArray(String[]::new));
        builder.observability().withErrorRouter(CrawlerErrorRouters.resolve(crawler));
        return builder.createWith(bodyNormalizer(source));
    }

    private static String required(Optional<String> value, String path) {
        return value.filter(text -> !text.isBlank())
                .orElseThrow(() -> new InvalidDescriptorException(path, "is required for a " + TYPE + " crawler"));
    }

    @SuppressWarnings("unchecked")
    private static Normalizer<byte[]> bodyNormalizer(SourceDescriptor source) {
        return (Normalizer<byte[]>) InfrastructurePluginFactory.normalizer(source);
    }
}
