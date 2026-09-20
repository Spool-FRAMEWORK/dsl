package software.spool.dsl.providers.crawler;

import software.spool.core.utils.routing.ErrorRouter;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.ErrorRouterDescriptor;
import software.spool.dsl.providers.KnownPlugins;
import software.spool.infrastructure.spi.provider.ErrorRouterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.Map;

/**
 * Finds the error router a crawler descriptor asks for, for the poll and the stream providers alike.
 */
final class CrawlerErrorRouters {

    private CrawlerErrorRouters() {}

    /**
     * Asks the plugin named by {@code errorRouter.type} for its router, giving it the configuration of the
     * descriptor and the id of the module.
     *
     * @param crawler the crawler descriptor
     * @return the router, or {@code null} when the descriptor names none, which leaves the crawler its default
     */
    static ErrorRouter resolve(CrawlerDescriptor crawler) {
        ErrorRouterDescriptor descriptor = crawler.errorRouter();
        if (descriptor == null) return null;
        PluginConfiguration configuration = PluginConfiguration.of(descriptor.configuration(), Map.of("moduleId", crawler.id()));
        return KnownPlugins.get(ErrorRouterProvider.class, descriptor.type(), "errorRouter.type", "error router")
                .create(configuration);
    }
}
