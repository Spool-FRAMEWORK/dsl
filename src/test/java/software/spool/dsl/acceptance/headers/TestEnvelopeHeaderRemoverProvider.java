package software.spool.dsl.acceptance.headers;

import software.spool.core.port.datalake.EnvelopeHeaderRemover;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderRemoverProvider;

@SpoolPlugin(EnvelopeHeaderRemoverProvider.class)
public class TestEnvelopeHeaderRemoverProvider implements EnvelopeHeaderRemoverProvider {
    @Override
    public String name() {
        return TestEnvelopeHeaderStore.TYPE;
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
    public EnvelopeHeaderRemover create(PluginConfiguration configuration) {
        return TestEnvelopeHeaderStore.shared();
    }
}
