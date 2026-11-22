package software.spool.dsl.acceptance.headers;

import software.spool.core.port.datalake.EnvelopeHeaderWriter;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderWriterProvider;

@SpoolPlugin(EnvelopeHeaderWriterProvider.class)
public class TestEnvelopeHeaderWriterProvider implements EnvelopeHeaderWriterProvider {
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
    public EnvelopeHeaderWriter create(PluginConfiguration configuration) {
        return TestEnvelopeHeaderStore.shared();
    }
}
