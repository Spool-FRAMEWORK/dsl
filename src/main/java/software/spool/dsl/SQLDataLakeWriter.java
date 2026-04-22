package software.spool.dsl;

import software.spool.core.model.event.ItemPublished;
import software.spool.core.model.vo.IdempotencyKey;
import software.spool.core.model.vo.InboxItem;
import software.spool.core.model.vo.PartitionKey;
import software.spool.ingester.api.port.DataLakeWriter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Stream;

public class SQLDataLakeWriter implements DataLakeWriter {
    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS items (
                idempotency_key TEXT        NOT NULL PRIMARY KEY,
                partition_key   TEXT        NOT NULL,
                payload         JSONB       NOT NULL,
                ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """;

    // Si el mismo hash llega dos veces, se ignora silenciosamente (idempotente)
    private static final String UPSERT = """
            INSERT INTO items (idempotency_key, partition_key, payload)
            VALUES (?, ?, ?::jsonb)
            ON CONFLICT (idempotency_key) DO NOTHING
            """;

    private final DataSource dataSource;

    public SQLDataLakeWriter(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    @Override
    public Stream<IdempotencyKey> write(Collection<InboxItem> items) {
        if (items == null || items.isEmpty()) return Stream.of();

        try (Connection conn = openConnection();
             PreparedStatement stmt = conn.prepareStatement(UPSERT)) {
            conn.setAutoCommit(false);
            for (InboxItem item : items) {
                stmt.setString(1, item.idempotencyKey().value());
                stmt.setString(2, PartitionKey.of(item.partitionKeySchema()).from(item.payload()).value());
                stmt.setString(3, item.payload());
                stmt.addBatch();
            }
            int[] results = stmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to write items to PostgreSQL DataLake", e);
        }
        return items.stream().map(InboxItem::idempotencyKey);
    }

    private void initSchema() {
        try (Connection conn = openConnection();
             PreparedStatement stmt = conn.prepareStatement(CREATE_TABLE)) {
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize DataLake schema", e);
        }
    }

    private Connection openConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private static int countInserted(int[] results) {
        int count = 0;
        for (int r : results) if (r > 0) count++;
        return count;
    }

    private static int countSkipped(int[] results) {
        int count = 0;
        for (int r : results) if (r == 0) count++;
        return count;
    }
}
