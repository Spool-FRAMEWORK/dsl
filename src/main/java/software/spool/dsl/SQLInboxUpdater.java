package software.spool.dsl;

import software.spool.core.exception.InboxUpdateException;
import software.spool.core.model.InboxItemStatus;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;
import software.spool.core.model.vo.PartitionKeySchema;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.core.port.serde.NamingConvention;
import software.spool.core.port.serde.PayloadDeserializer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;

public class SQLInboxUpdater implements InboxUpdater {

    private static final String UPDATE = """
            UPDATE inbox SET status = ?
            WHERE idempotency_key = ?
            RETURNING idempotency_key, metadata, partition_key_schema, payload, status, timestamp
            """;

    private final DataSource dataSource;

    private final PayloadDeserializer<EventMetadata> metadataDeserializer =
            NamingConvention.CAMEL_CASE.deserializerFor(EventMetadata.class);
    private final PayloadDeserializer<PartitionKeySchema> partitionKeySchemaDeserializer =
            NamingConvention.CAMEL_CASE.deserializerFor(PartitionKeySchema.class);

    public SQLInboxUpdater(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public InboxItem update(IdempotencyKey idempotencyKey, InboxItemStatus status) throws InboxUpdateException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE)) {

            ps.setString(1, status.name());
            ps.setString(2, idempotencyKey.value());

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new InboxUpdateException(idempotencyKey, "No inbox item found for key");
                }

                EventMetadata metadata = metadataDeserializer.deserialize(rs.getString("metadata"));
                PartitionKeySchema partitionKeySchema =
                        partitionKeySchemaDeserializer.deserialize(rs.getString("partition_key_schema"));

                String payload = rs.getString("payload");

                InboxItemStatus updatedStatus = InboxItemStatus.valueOf(rs.getString("status"));
                Instant timestamp = rs.getTimestamp("timestamp").toInstant();

                return new InboxItem(
                        idempotencyKey,
                        metadata,
                        partitionKeySchema,
                        payload,
                        updatedStatus,
                        timestamp
                );
            }

        } catch (InboxUpdateException e) {
            throw e;
        } catch (Exception e) {
            throw new InboxUpdateException(idempotencyKey, e.getMessage(), e);
        }
    }
}
