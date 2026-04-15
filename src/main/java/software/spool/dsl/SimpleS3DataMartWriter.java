package software.spool.dsl;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.UUID;
import java.util.stream.Stream;

public class SimpleS3DataMartWriter implements DataMartWriter<AverageTickerPayload> {
    private final S3Client s3;
    private final String bucket;

    public SimpleS3DataMartWriter(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public void write(MountTarget target, Stream<PartitionedRecord<AverageTickerPayload>> records) {
        records.forEach(record -> {
            String s3Path = target.dataMart() + "/" + record.partitionKey().value() + "/" + UUID.randomUUID() + ".txt";
            s3.putObject(req -> req.bucket(bucket).key(s3Path),
                    RequestBody.fromString(RecordSerializerFactory.record().serialize(record.record())));
        });
    }
}