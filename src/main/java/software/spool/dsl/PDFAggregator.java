package software.spool.dsl;

import events.SyntheaReceived;
import software.spool.mounter.api.model.AggregatedRecord;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.MountAggregator;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.Map;
import java.util.stream.Stream;

public class PDFAggregator implements MountAggregator<SyntheaReceived> {
    @Override
    public Stream<AggregatedRecord<SyntheaReceived>> aggregate(Stream<PartitionedRecord<GenericRecord>> records) {
        return records.map(PartitionedRecord::record)
                .peek(System.out::println)
                .map(b -> b.getNested("payload").get())
                .map(m -> new AggregatedRecord<>(GenericRecord.of(Map.of()), new SyntheaReceived(m.getString("PATIENT").get(), m.getString("_type").get())));
    }
}
