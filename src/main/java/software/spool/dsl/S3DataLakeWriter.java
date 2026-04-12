package software.spool.dsl;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.PartitionKey;
import software.spool.ingester.api.port.DataLakeWriter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class S3DataLakeWriter implements DataLakeWriter {
    private static final String CONTENT_TYPE = "application/json";
    private final S3Client s3Client;
    private final String bucket;

    public S3DataLakeWriter(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<ItemPublished> items) {
        List<IdempotencyKey> written = new ArrayList<>();
        for (ItemPublished item : items) {
            try {
                byte[] payload = item.payload().getBytes(StandardCharsets.UTF_8);
                String key = buildKey(item);
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType(CONTENT_TYPE)
                        .contentLength((long) payload.length)
                        .build();
                s3Client.putObject(request, RequestBody.fromBytes(payload));
                written.add(item.idempotencyKey());
            } catch (Exception e) {
                System.err.println(String.format("Failed to write item %s to S3, skipping", item.idempotencyKey()));
            }
        }
        return written.stream();
    }

    private String buildKey(ItemPublished item) {
        String partition = PartitionKey.of(item.partitionKeySchema()).from(item.payload()).value();
        String filename = item.idempotencyKey().value();
        return partition.replace("::", "/") + "/" + filename;
    }
}