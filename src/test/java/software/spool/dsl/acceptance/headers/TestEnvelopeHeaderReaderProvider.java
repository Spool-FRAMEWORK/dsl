package software.spool.dsl.acceptance.headers;

import software.spool.core.port.datalake.EnvelopeHeaderReader;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderReaderProvider;

@SpoolPlugin(EnvelopeHeaderReaderProvider.class)
public class TestEnvelopeHeaderReaderProvider implements EnvelopeHeaderReaderProvider {
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
    public EnvelopeHeaderReader create(PluginConfiguration configuration) {
        return TestEnvelopeHeaderStore.shared();
    }
}
