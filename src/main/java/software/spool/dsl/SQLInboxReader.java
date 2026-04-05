package software.spool.dsl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.spool.core.adapter.jackson.DomainMapperFactory;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.exception.InboxReadException;
import software.spool.core.model.InboxItemStatus;
import software.spool.core.model.vo.EventMetadata;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;
import software.spool.core.model.vo.PartitionKeySchema;
import software.spool.feeder.api.port.InboxReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class SQLInboxReader implements InboxReader {

    private static final String SELECT_AND_LOCK = """
            UPDATE inbox SET status = 'PUBLISHING'
            WHERE idempotency_key IN (
                SELECT idempotency_key FROM inbox
                WHERE status = ?
                FOR UPDATE SKIP LOCKED
            )
            RETURNING idempotency_key, status, metadata, partition_key_schema, payload, timestamp
            """;

    private final DataSource dataSource;
    private final ObjectMapper mapper;

    public SQLInboxReader(DataSource dataSource) {
        this.dataSource = dataSource;
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public Stream<InboxItem> findByStatus(InboxItemStatus status) throws InboxReadException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_AND_LOCK)) {

            ps.setString(1, status.name());
            ResultSet rs = ps.executeQuery();

            List<InboxItem> items = new ArrayList<>();
            while (rs.next()) {
                items.add(new InboxItem(
                        new IdempotencyKey(rs.getString("idempotency_key")),
                        PayloadDeserializerFactory.json().as(EventMetadata.class).deserialize(rs.getString("metadata")),
                        DomainMapperFactory.camelCase(PartitionKeySchema.class).deserialize(rs.getString("partition_key_schema")),
                        rs.getString("payload"),
                        InboxItemStatus.valueOf(rs.getString("status")),
                        rs.getTimestamp("timestamp").toInstant()
                ));
            }

            return items.stream();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new InboxReadException("Failed to read inbox items: " + e.getMessage(), e);
        }
    }
}