package software.spool.dsl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.spool.core.adapter.sql.SQLInboxUpdater;
import software.spool.core.port.inbox.InboxUpdater;
import software.spool.dsl.descriptors.infrastructure.InboxDescriptor;

class InboxUpdaterFactory {
    public static InboxUpdater from(InboxDescriptor descriptor) {
        return switch (descriptor.type()) {
            case SQL -> sql(descriptor.sql().type(), descriptor.sql().host(), descriptor.sql().database(), descriptor.sql().user(), descriptor.sql().password());
            case IN_MEMORY -> null;
            case CUSTOM -> null;
        };
    }

    public static InboxUpdater sql(String dbType, String host, String database, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:%s://%s/%s", dbType, host, database));
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(5);

        HikariDataSource dataSource = new HikariDataSource(config);
        return new SQLInboxUpdater(dataSource);
    }
}
