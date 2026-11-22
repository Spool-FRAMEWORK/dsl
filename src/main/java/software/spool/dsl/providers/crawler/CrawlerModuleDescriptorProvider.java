package software.spool.dsl.providers.crawler;

import software.spool.core.model.schedule.Schedule;
import software.spool.core.model.vo.MediaType;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.core.port.serde.NamingConvention;
import software.spool.core.utils.media.MediaTypeResolver;
import software.spool.dsl.descriptors.module.ScheduleReader;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.WebhookDescriptor;
import software.spool.dsl.descriptors.module.crawler.ErrorRouterDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.NormalizerDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.providers.ModuleDescriptorProvider;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParser;
import software.spool.dsl.reader.ValueParsers;
import software.spool.infrastructure.spi.SpoolPlugin;

import java.util.List;

import static software.spool.dsl.reader.ReadableDuration.insteadOfMilliseconds;

@SpoolPlugin(ModuleDescriptorProvider.class)
public class CrawlerModuleDescriptorProvider implements ModuleDescriptorProvider {
    private static final ScheduleReader SCHEDULE = ScheduleReader.standard();

    /** An alias such as JSON or a full media type such as application/json, which is what the records are labelled. */
    private static final ValueParser<MediaType> MEDIA_TYPE = ValueParsers.of("a media type such as JSON or application/json",
            raw -> ValueParsers.STRING.parse(raw).map(MediaTypeResolver::resolve).filter(type -> type.value().contains("/")));

    @Override
    public String name() {
        return "CRAWLER";
    }

    @Override
    public SpoolModuleDescriptor read(DescriptorReader crawler) {
        return new CrawlerDescriptor(
                crawler.string("type"),
                crawler.string("id"),
                readSource(crawler.object("source")),
                readEventMapping(crawler.object("eventMapping")),
                readErrorRouter(crawler),
                SCHEDULE.read(crawler).orElseGet(Schedule::once),
                readWebhook(crawler.objectOrEmpty("webhook"))
        );
    }

    private static WebhookDescriptor readWebhook(DescriptorReader webhook) {
        return new WebhookDescriptor(
                webhook.optional("authentication", ValueParsers.STRING),
                webhook.optional("role", ValueParsers.STRING),
                webhook.optional("maxBodySize", ValueParsers.INTEGER));
    }

    private static ErrorRouterDescriptor readErrorRouter(DescriptorReader crawler) {
        if (!crawler.keys().contains("errorRouter")) return null;
        DescriptorReader router = crawler.object("errorRouter");
        return new ErrorRouterDescriptor(router.string("type"), router.stringMap("configuration"));
    }

    private static SourceDescriptor readSource(DescriptorReader source) {
        source.objectOrEmpty("configuration").replaced("scheduleMilliseconds", insteadOfMilliseconds("schedule.every"));
        NormalizerDescriptor normalizer = NormalizerDescriptorReader.read(source);
        return new SourceDescriptor(
                source.string("type"),
                source.optional("id", ValueParsers.STRING).orElse(null),
                source.stringMap("configuration"),
                normalizer,
                source.required("mediaType", MEDIA_TYPE),
                source.optional("rootPath", ValueParsers.STRING).orElse(null),
                source.list("enrichment").stream().map(CrawlerModuleDescriptorProvider::readRule).toList()
        );
    }

    private static EnrichmentRule readRule(DescriptorReader rule) {
        return new EnrichmentRule(rule.string("source"), rule.optional("target", ValueParsers.STRING).orElse(null));
    }

    private static EventMappingDescriptor readEventMapping(DescriptorReader mapping) {
        return new EventMappingDescriptor(
                mapping.required("namingConvention", ValueParsers.oneOf(NamingConvention.class)),
                EventClasses.in(mapping.optional("eventPackage", ValueParsers.STRING).orElse(EventClasses.DEFAULT_PACKAGE))
                        .resolve(mapping.path() + ".domainMappingList", mapping.optional("domainMappingList", ValueParsers.TEXTS).orElse(List.of())),
                mapping.optional("attributeList", ValueParsers.TEXTS).orElse(List.of())
        );
    }
}
