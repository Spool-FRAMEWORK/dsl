package software.spool.dsl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.spool.core.exception.InboxUpdateException;
import software.spool.core.model.InboxItemStatus;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;
import software.spool.core.model.vo.PartitionKeySchema;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.port.serde.NamingConvention;
import software.spool.core.port.serde.PayloadDeserializer;

import java.time.Instant;

/**
 * S3-backed InboxUpdater.
 *
 * Mueve el objeto del prefijo de estado actual al nuevo:
 *   inbox/{OLD_STATUS}/{idempotencyKey}.json → inbox/{NEW_STATUS}/{idempotencyKey}.json
 *
 * Para localizar el estado actual hace HeadObject por cada valor del enum
 * InboxItemStatus (O(n) con n = número de estados, típicamente ≤ 5).
 */
public class S3InboxUpdater implements InboxUpdater {

    private static final String INBOX_PREFIX = "inbox/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper mapper;

    private final PayloadDeserializer metadataDeserializer =
            NamingConvention.CAMEL_CASE.deserializerFor(EventMetadata.class);
    private final PayloadDeserializer partitionKeySchemaDeserializer =
            NamingConvention.CAMEL_CASE.deserializerFor(PartitionKeySchema.class);

    public S3InboxUpdater(S3Client s3Client, String bucketName) {
        this.s3Client   = s3Client;
        this.bucketName = bucketName;
        this.mapper     = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public InboxItem update(IdempotencyKey idempotencyKey, InboxItemStatus newStatus)
            throws InboxUpdateException {
        try {
            String sourceKey = findCurrentKey(idempotencyKey);
            if (sourceKey == null) {
                throw new InboxUpdateException(
                        idempotencyKey,
                        "Item not found in S3 inbox: " + idempotencyKey.value(),
                        null
                );
            }

            String destinationKey = INBOX_PREFIX + newStatus.name() + "/"
                    + idempotencyKey.value() + ".json";

            // Leer el contenido actual
            ResponseBytes<GetObjectResponse> raw = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(sourceKey)
                            .build()
            );
            InboxItemDto dto = mapper.readValue(raw.asByteArray(), InboxItemDto.class);

            s3Client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucketName).sourceKey(sourceKey)
                    .destinationBucket(bucketName).destinationKey(destinationKey)
                    .build()
            );
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName).key(sourceKey)
                    .build()
            );

            EventMetadata metadata = (EventMetadata) metadataDeserializer.deserialize(dto.metadata());
            PartitionKeySchema partitionKeySchema =
                    (PartitionKeySchema) partitionKeySchemaDeserializer.deserialize(dto.partitionKeySchema());

            return new InboxItem(
                    idempotencyKey,
                    metadata,
                    partitionKeySchema,
                    dto.payload(),
                    newStatus,
                    dto.timestamp()
            );

        } catch (InboxUpdateException e) {
            throw e;
        } catch (Exception e) {
            throw new InboxUpdateException(idempotencyKey, e.getMessage(), e);
        }
    }

    /**
     * Localiza la clave S3 del ítem probando HEAD en cada prefijo de estado.
     * Devuelve null si no existe en ninguno.
     */
    private String findCurrentKey(IdempotencyKey idempotencyKey) {
        for (InboxItemStatus status : InboxItemStatus.values()) {
            String candidate = INBOX_PREFIX + status.name() + "/"
                    + idempotencyKey.value() + ".json";
            try {
                s3Client.headObject(HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(candidate)
                        .build()
                );
                return candidate;
            } catch (NoSuchKeyException ignored) {
                // No está en este estado, probar el siguiente
            }
        }
        return null;
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
