package software.spool.dsl;

import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.DataLakeReader;
import software.spool.mounter.api.port.PartitionedRecord;

import java.util.List;

public class SimpleS3DataLakeReader implements DataLakeReader<TickerPayload> {
    private final S3Client s3;
    private final String bucket;

    public SimpleS3DataLakeReader(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public List<PartitionedRecord<TickerPayload>> read(PartitionKey key) {
        return s3.listObjectsV2(req -> req.bucket(bucket).prefix(key.value()))
                .contents().stream()
                .map(s3Object -> {
                    String objectKey = s3Object.key();
                    String extractedPartitionPath = objectKey.contains("/")
                            ? objectKey.substring(0, objectKey.lastIndexOf('/'))
                            : objectKey;
                    PartitionKey realItemPartitionKey = new PartitionKey(extractedPartitionPath);
                    String content = s3.getObjectAsBytes(req -> req.bucket(bucket).key(objectKey))
                            .asUtf8String();
                    return new PartitionedRecord<>(realItemPartitionKey, content);
                })
                .map(p -> {
                    TickerPayload payload = PayloadDeserializerFactory.json()
                            .as(TickerPayload.class)
                            .deserialize(p.record());
                    return new PartitionedRecord<>(p.partitionKey(), payload);
                })
                .toList();
    }
}