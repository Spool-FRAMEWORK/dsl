package software.spool.dsl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.spool.dsl.descriptors.infrastructure.InboxDescriptor;
import software.spool.feeder.api.port.InboxReader;

class InboxReaderFactory {
    public static InboxReader from(InboxDescriptor descriptor) {
        return switch (descriptor.type()) {
            case SQL -> sql(descriptor.sql().type(), descriptor.sql().host(), descriptor.sql().database(), descriptor.sql().user(), descriptor.sql().password());
            case IN_MEMORY -> null;
            case CUSTOM -> null;
        };
    }

    public static InboxReader sql(String dbType, String host, String database, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:%s://%s/%s", dbType, host, database));
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(5);

        HikariDataSource dataSource = new HikariDataSource(config);
        return new SQLInboxReader(dataSource);
    }
}
