package software.spool.dsl.acceptance.mounter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class AmountTotal implements MountAggregator<AmountTotal.Total> {
    private static final PartitionKey ALL_PAYMENTS = PartitionKey.ofEntriesWithoutDate().with("scope", "all").build();

    public record Total(long totalAmount) {
    }

    @Override
    public Stream<PartitionedRecord<Total>> aggregate(Stream<PartitionedRecord<GenericRecord>> records) {
        List<GenericRecord> all = records.map(PartitionedRecord::record).toList();
        long total = all.stream().mapToLong(AmountTotal::amountOf).sum();
        return Stream.of(new PartitionedRecord<>(ALL_PAYMENTS, new Total(total)));
    }

    @SuppressWarnings("unchecked")
    private static long amountOf(GenericRecord record) {
        Map<String, Object> payload = (Map<String, Object>) record.get("payload");
        return ((Number) payload.get("amount")).longValue();
    }
}
