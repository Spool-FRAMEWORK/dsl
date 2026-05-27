package software.spool.dsl;

import events.SyntheaReceived;
import events.TestEvent;
import events.TimeStampEvent;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.model.vo.PartitionKey;
import software.spool.core.port.bus.EventBus;
import software.spool.core.utils.polling.PollingPolicy;
import software.spool.core.utils.polling.ThreadedPollingScheduler;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;
import software.spool.infrastructure.spi.provider.dataLake.PartitionedReaderProvider;
import software.spool.infrastructure.spi.provider.datamart.DataMartWriterProvider;
import software.spool.mounter.api.Mounter;
import software.spool.mounter.api.adapter.AlwaysClosedWindowPolicy;
import software.spool.mounter.api.adapter.NoOpMountCheckpoint;
import software.spool.mounter.api.builder.MounterBuilderFactory;
import software.spool.mounter.api.model.GenericRecord;
import software.spool.mounter.api.port.DataMartWriter;
import software.spool.mounter.api.port.MountPartitionSchema;
import software.spool.mounter.api.port.MountTarget;
import software.spool.mounter.api.port.PartitionedRecord;
import software.spool.mounter.api.utils.MounterErrorRouter;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws IOException {
        EventBus bus = PluginResolver.get(EventBusProvider.class, "IN_MEMORY").create(PluginConfiguration.empty());
        bus.subscribe(SyntheaReceived.class, System.out::println);
        SpoolNode node = SpoolNode.create();
        Mounter mounter = MounterBuilderFactory.polling(PluginResolver.resolve(PartitionedReaderProvider.class, PluginConfiguration.builder().with("path", "D:/spool/datalake").build()))
                .aggregatingWith(new PDFAggregator())
                .onTarget(MountTarget.transformation("BOE", new PartitionKey("year=2026::hour=17")))
                .partitionWindowPolicy(new AlwaysClosedWindowPolicy())
                .partitioningWith((input, output, target) -> target.sourceKey())
                .pollingWith(PollingPolicy.ONCE)
                .scheduledWith(new ThreadedPollingScheduler())
                .checkpoint(new NoOpMountCheckpoint())
                .writingWith((DataMartWriter<SyntheaReceived>) PluginResolver.resolve(DataMartWriterProvider.class, PluginConfiguration.builder().with("path", "D:/spool/datalake").build()))
                .errorRouting(MounterErrorRouter.defaults(bus))
                .build();
        node.register(mounter).start();
//        SpoolNodeDSL.fromDescriptor("/Spool.yaml").start();
//        List<? extends PartitionedRecord<?>> list = PluginResolver.resolve(PartitionedReaderProvider.class, PluginConfiguration.builder().with("path", "D:/spool/datalake").build())
//                .read(new PartitionKey("year=2026::hour=17"));
//        System.out.println(list);
    }
}