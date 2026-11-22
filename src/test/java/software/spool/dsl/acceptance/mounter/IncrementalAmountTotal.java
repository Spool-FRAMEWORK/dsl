package software.spool.dsl.acceptance.mounter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.IncrementalMountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.Map;

public class IncrementalAmountTotal implements IncrementalMountAggregator<IncrementalAmountTotal.Total, IncrementalAmountTotal.Total> {
    private static final PartitionKey ALL_PAYMENTS = PartitionKey.ofEntriesWithoutDate().with("scope", "all").build();

    public record Total(long totalAmount) {
    }

    @Override
    public PartitionKey groupOf(PartitionedRecord<GenericRecord> record) {
        return ALL_PAYMENTS;
    }

    @Override
    public Total initial() {
        return new Total(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Total add(Total state, GenericRecord record) {
        Map<String, Object> payload = (Map<String, Object>) record.get("payload");
        return new Total(state.totalAmount() + ((Number) payload.get("amount")).longValue());
    }

    @Override
    public Total result(Total state) {
        return state;
    }

    @Override
    public Class<Total> stateType() {
        return Total.class;
    }
}
