package software.spool.dsl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.spool.crawler.api.adapter.InMemoryInboxWriter;
import software.spool.crawler.api.port.InboxWriter;
import software.spool.dsl.descriptors.infrastructure.InboxDescriptor;
import software.spool.ingester.api.port.DataLakeWriter;

import java.net.URI;

public class InboxWriterFactory {
    public static InboxWriter from(InboxDescriptor descriptor) {
        return switch (descriptor.type()) {
            case SQL -> sql(descriptor.sql().type(), descriptor.sql().host(), descriptor.sql().database(), descriptor.sql().user(), descriptor.sql().password());
            case S3 -> s3(descriptor.s3().bucket(),
                    descriptor.s3().region(),
                    descriptor.s3().endpoint());
            case IN_MEMORY -> new InMemoryInboxWriter();
            case CUSTOM -> null;
        };
    }

    public static InboxWriter sql(String dbType, String host, String database, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:%s://%s/%s", dbType, host, database));
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(5);

        HikariDataSource dataSource = new HikariDataSource(config);
        return new SQLInboxWriter(dataSource);
    }

    public static S3InboxWriter s3(String bucket, String region, String endpoint) {
        S3Client s3Client = buildS3Client(region, endpoint);
        return new S3InboxWriter(s3Client, bucket);
    }

    private static S3Client buildS3Client(String region, String endpoint) {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(region));

        if (endpoint != null && !endpoint.isBlank()) {
            builder
                    .endpointOverride(URI.create(endpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")
                    ))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build());
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        return builder.build();
    }
}
