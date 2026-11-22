package software.spool.dsl.acceptance.headers;

import software.spool.core.model.vo.EnvelopeHeader;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.datalake.EnvelopeHeaderReader;
import software.spool.core.port.datalake.EnvelopeHeaderRemover;
import software.spool.core.port.datalake.EnvelopeHeaderWriter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * A store of envelope headers for the scenarios only, shared by every provider of type {@value #TYPE}, so a scenario
 * can look into what the modules of a node kept in it.
 */
public final class TestEnvelopeHeaderStore implements EnvelopeHeaderWriter, EnvelopeHeaderReader, EnvelopeHeaderRemover {
    public static final String TYPE = "TEST_HEADERS";
    private static final TestEnvelopeHeaderStore SHARED = new TestEnvelopeHeaderStore();

    private final Map<IdempotencyKey, StoredHeader> headers = new ConcurrentHashMap<>();

    private TestEnvelopeHeaderStore() {
    }

    public static TestEnvelopeHeaderStore shared() {
        return SHARED;
    }

    public void clear() {
        headers.clear();
    }

    public List<StoredHeader> all() {
        return List.copyOf(headers.values());
    }

    public Optional<StoredHeader> stored(IdempotencyKey idempotencyKey) {
        return Optional.ofNullable(headers.get(idempotencyKey));
    }

    @Override
    public void write(PartitionKey partition, EnvelopeHeader header) {
        headers.put(header.idempotencyKey(), new StoredHeader(partition, header));
    }

    @Override
    public Optional<EnvelopeHeader> find(IdempotencyKey idempotencyKey) {
        return stored(idempotencyKey).map(StoredHeader::header);
    }

    @Override
    public Stream<EnvelopeHeader> findIn(PartitionKey partition) {
        return all().stream().filter(stored -> stored.partition().equals(partition)).map(StoredHeader::header);
    }

    @Override
    public void remove(PartitionKey partition, IdempotencyKey idempotencyKey) {
        headers.computeIfPresent(idempotencyKey, (key, stored) -> stored.partition().equals(partition) ? null : stored);
    }

    public record StoredHeader(PartitionKey partition, EnvelopeHeader header) {
    }
}
