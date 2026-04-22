package software.spool.dsl;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.spool.core.adapter.otel.OTELConfig;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.port.MountPartitionSchema;
import software.spool.mounter.api.port.MountTarget;

import java.net.URI;

public class Main {
//    public static void main(String[] args) throws Exception {
//        OTELConfig.init("mounter");
//
//        S3Client s3Client = S3Client.builder()
//                .endpointOverride(URI.create("http://localhost:4566"))
//                .region(Region.US_EAST_1)
//                .credentialsProvider(StaticCredentialsProvider.create(
//                        AwsBasicCredentials.create("test-key", "test-secret")))
//                .forcePathStyle(true)
//                .build();
//        String bucket = "spool-datalake";
//
//        var reader = new SimpleS3DataLakeReader(s3Client, bucket);
//        var writer = new SimpleS3DataMartWriter(s3Client, bucket);
//
//        // 3. Definimos un target y esquema de prueba para Spool
//        MountTarget target = MountTarget.of("test-datamart", new PartitionKey("year=2026/month=04/day=14"));
//        MountPartitionSchema<AverageTickerPayload> schema = MountPartitionSchema.of(AverageTickerPayload.class, "year", "month", "symbol");
//
//        // 4. Inicializamos usando tu MounterBuilderFactory y los adaptadores NoOp que ya tienes
//        Mounter mounter = MounterBuilderFactory.polling(reader)
//                .aggregatingWith(new AverageAggregator()) // Pasa los datos tal cual
//                .writingWith(writer)
//                .onTarget(target)
//                .partitionWindowPolicy(new AlwaysClosedWindowPolicy()) // Siempre lee todo
//                .checkpoint(new NoOpMountCheckpoint()) // No guarda estado
//                .partitioningWith(schema)
//                .pollingWith(PollingPolicy.ONCE)
//                .emittingWith(EventBusFactory.kafkaEmitter("http://localhost:9092"))
//                .build();
//
//        SpoolNode.create()
//                .register(mounter)
//                .start();
//    }
}