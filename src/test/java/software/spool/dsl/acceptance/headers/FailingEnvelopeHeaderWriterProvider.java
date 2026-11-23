package software.spool.dsl.acceptance.headers;

import software.spool.core.exception.EnvelopeHeaderStoreException;
import software.spool.core.port.datalake.EnvelopeHeaderWriter;
import software.spool.infrastructure.spi.SpoolPlugin;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.dataLake.EnvelopeHeaderWriterProvider;

/**
 * A store of envelope headers for the scenarios only, of type {@value #TYPE}, that refuses every header.
 */
@SpoolPlugin(EnvelopeHeaderWriterProvider.class)
public class FailingEnvelopeHeaderWriterProvider implements EnvelopeHeaderWriterProvider {
    public static final String TYPE = "FAILING_HEADERS";

    @Override
    public String name() {
        return TYPE;
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
        return (partition, header) -> {
            throw new EnvelopeHeaderStoreException("The store refuses the header of envelope "
                    + header.idempotencyKey().value(), new IllegalStateException("refused by the scenario"));
        };
    }
}
