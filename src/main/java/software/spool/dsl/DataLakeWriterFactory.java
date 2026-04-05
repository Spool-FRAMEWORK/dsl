package software.spool.dsl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.spool.dsl.descriptors.infrastructure.DataLakeDescriptor;
import software.spool.ingester.api.port.DataLakeWriter;

class DataLakeWriterFactory {
    public static DataLakeWriter from(DataLakeDescriptor descriptor) {
        return switch (descriptor.type()) {
            case SQL -> sql(descriptor.sql().type(), descriptor.sql().host(), descriptor.sql().database(), descriptor.sql().user(), descriptor.sql().password());
            case IN_MEMORY -> System.out::println;
            case CUSTOM -> null;
        };
    }

    public static DataLakeWriter sql(String dbType, String host, String database, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:%s://%s/%s", dbType, host, database));
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(5);

        HikariDataSource dataSource = new HikariDataSource(config);
        return new SQLDataLakeWriter(dataSource);
    }
}
