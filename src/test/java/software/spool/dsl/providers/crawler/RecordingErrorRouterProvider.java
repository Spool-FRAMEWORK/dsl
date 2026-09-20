package software.spool.dsl.providers.crawler;

import software.spool.core.utils.routing.ErrorRouter;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.ErrorRouterProvider;
import software.spool.infrastructure.spi.provider.PluginConfiguration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** An error router written in code, as an application would, that remembers how it was asked for. */
@SpoolPlugin(ErrorRouterProvider.class)
public class RecordingErrorRouterProvider implements ErrorRouterProvider {

    static final List<PluginConfiguration> CREATED = new CopyOnWriteArrayList<>();

    @Override
    public String name() {
        return "TEST_ALERTS";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean supports(PluginConfiguration configuration) {
        return true;
    }

    @Override
    public ErrorRouter create(PluginConfiguration configuration) {
        CREATED.add(configuration);
        return new ErrorRouter();
    }
}
