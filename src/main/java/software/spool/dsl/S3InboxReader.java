package software.spool.dsl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.spool.core.adapter.jackson.DomainMapperFactory;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.exception.InboxReadException;
import software.spool.core.model.InboxItemStatus;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;
import software.spool.core.model.vo.PartitionKeySchema;
import software.spool.feeder.api.port.InboxReader;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class S3InboxReader implements InboxReader {

    private static final String INBOX_PREFIX = "inbox/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;

    public S3InboxReader(S3Client s3Client, String bucketName) {
        this.s3Client  = s3Client;
        this.bucketName = bucketName;
        this.mapper    = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public Stream<InboxItem> findByStatus(InboxItemStatus status) throws InboxReadException {
        String sourcePrefix     = INBOX_PREFIX + status.name() + "/";
        String publishingPrefix = INBOX_PREFIX + InboxItemStatus.PUBLISHING.name() + "/";

        try {
            ListObjectsV2Response listing = s3Client.listObjectsV2(
                    ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(sourcePrefix)
                            .build()
            );

            List<InboxItem> items = new ArrayList<>();

            for (S3Object s3Obj : listing.contents()) {
                String sourceKey      = s3Obj.key();
                String fileName       = sourceKey.substring(sourcePrefix.length());
                String destinationKey = publishingPrefix + fileName;

                try {
                    s3Client.copyObject(CopyObjectRequest.builder()
                            .sourceBucket(bucketName).sourceKey(sourceKey)
                            .destinationBucket(bucketName).destinationKey(destinationKey)
                            .build()
                    );
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName).key(sourceKey)
                            .build()
                    );
                } catch (S3Exception e) {
                    continue;
                }

                ResponseBytes<GetObjectResponse> raw = s3Client.getObjectAsBytes(
                        GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(destinationKey)
                                .build()
                );

                InboxItemDto dto = mapper.readValue(raw.asByteArray(), InboxItemDto.class);

                items.add(new InboxItem(
                        new IdempotencyKey(dto.idempotencyKey()),
                        PayloadDeserializerFactory.json().as(EventMetadata.class).deserialize(dto.metadata()),
                        DomainMapperFactory.camelCase(PartitionKeySchema.class).deserialize(dto.partitionKeySchema()),
                        dto.payload(),
                        InboxItemStatus.PUBLISHING,
                        dto.timestamp()
                ));
            }

            return items.stream();

        } catch (InboxReadException e) {
            throw e;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new InboxReadException("Failed to read inbox items from S3: " + e.getMessage(), e);
        }
    }

    record InboxItemDto(
            String idempotencyKey,
            String metadata,
            String partitionKeySchema,
            String payload,
            String status,
            Instant timestamp
    ) {}
}