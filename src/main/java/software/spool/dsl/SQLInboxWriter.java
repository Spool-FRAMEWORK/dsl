package software.spool.dsl;

import software.spool.core.adapter.jackson.RecordSerializerFactory;
import software.spool.core.exception.DuplicateEventException;
import software.spool.core.exception.InboxWriteException;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;
import software.spool.crawler.api.port.InboxWriter;

import javax.sql.DataSource;
import java.sql.*;

public class SQLInboxWriter implements InboxWriter {

    private static final String CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS inbox (
            idempotency_key     VARCHAR(255)    PRIMARY KEY,
            status              VARCHAR(50)     NOT NULL,
            metadata            TEXT,
            partition_key_schema TEXT,
            payload             TEXT            NOT NULL,
            timestamp           TIMESTAMP       NOT NULL
        )
        """;

    private static final String INSERT = """
        INSERT INTO inbox (idempotency_key, status, metadata, partition_key_schema, payload, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """;

    private final DataSource dataSource;

    public SQLInboxWriter(DataSource dataSource) {
        this.dataSource = dataSource;
        initTable();
    }

    private void initTable() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize inbox table", e);
        }
    }

    @Override
    public IdempotencyKey receive(InboxItem item) throws InboxWriteException, DuplicateEventException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT)) {

            ps.setString(1, item.idempotencyKey().value());
            ps.setString(2, item.status().name());
            ps.setString(3, RecordSerializerFactory.record().serialize(item.metadata()));
            ps.setString(4, RecordSerializerFactory.record().serialize(item.partitionKeySchema()));
            ps.setString(5, item.payload());
            ps.setTimestamp(6, Timestamp.from(item.timestamp()));

            ps.executeUpdate();
            return item.idempotencyKey();

        } catch (SQLIntegrityConstraintViolationException e) {
            throw new DuplicateEventException(item.idempotencyKey());
        } catch (SQLException e) {
            if ("23505".equals(e.getSQLState()))
                throw new DuplicateEventException(item.idempotencyKey());
            throw new InboxWriteException("Failed to write inbox item: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new InboxWriteException("Failed to serialize inbox item: " + e.getMessage(), e);
        }
    }

}
