package software.spool.dsl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.exception.InboxWriteException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;
import software.spool.crawler.api.port.InboxWriter;

import java.time.Instant;

/**
 * S3-backed InboxWriter.
 *
 * Layout de clave:
 *   inbox/{STATUS}/{idempotencyKey}.json
 *
 * La restricción PRIMARY KEY de SQL se replica con un HeadObject previo al PutObject:
 * si el objeto ya existe se lanza DuplicateEventException, igual que la violación
 * de constraint en la versión SQL.
 *
 * El JSON almacenado es compatible con S3InboxReader / S3InboxUpdater:
 * los campos metadata y partitionKeySchema se serializan con RecordSerializerFactory,
 * igual que en la versión SQL.
 */
public class S3InboxWriter implements InboxWriter {

    private static final String INBOX_PREFIX = "inbox/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;

    public S3InboxWriter(S3Client s3Client, String bucketName) {
        this.s3Client   = s3Client;
        this.bucketName = bucketName;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public IdempotencyKey receive(InboxItem item) throws InboxWriteException, DuplicateEventException {
        String objectKey = INBOX_PREFIX + item.status().name() + "/"
                + item.idempotencyKey().value() + ".json";

        // Equivalente al PRIMARY KEY constraint: objeto ya existente → DuplicateEventException
        if (objectExists(objectKey)) {
            throw new DuplicateEventException(item.idempotencyKey());
        }

        try {
            String metadataSerialized           = RecordSerializerFactory.record().serialize(item.metadata());
            String partitionKeySchemaSerialized = RecordSerializerFactory.record().serialize(item.partitionKeySchema());

            InboxItemDto dto = new InboxItemDto(
                    item.idempotencyKey().value(),
                    metadataSerialized,
                    partitionKeySchemaSerialized,
                    item.payload(),
                    item.status().name(),
                    item.timestamp()
            );

            byte[] body = mapper.writeValueAsBytes(dto);

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromBytes(body)
            );

            return item.idempotencyKey();

        } catch (DuplicateEventException e) {
            throw e;
        } catch (S3Exception e) {
            throw new InboxWriteException("Failed to write inbox item to S3: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new InboxWriteException("Failed to serialize inbox item: " + e.getMessage(), e);
        }
    }

    private boolean objectExists(String key) {
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build()
            );
            return true;
        } catch (NoSuchKeyException e) {
            return false;
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
