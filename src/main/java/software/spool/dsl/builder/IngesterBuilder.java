package software.spool.dsl.builder;

import com.mysql.cj.x.protobuf.MysqlxNotice;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.inbox.InboxReader;
import software.spool.dsl.DataLakeWriterFactory;
import software.spool.dsl.EventBusFactory;
import software.spool.dsl.InboxReaderFactory;
import software.spool.dsl.InboxUpdaterFactory;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.ingester.FlushIngesterDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.ingester.api.Ingester;
import software.spool.ingester.api.builder.IngesterBuilderFactory;
import software.spool.ingester.internal.utils.FlushPolicy;

import java.time.Duration;
import java.util.Objects;

public class IngesterBuilder {
    public static Ingester buildFrom(IngesterDescriptor ingester, InfrastructureDescriptor infrastructure) {
        EventBus eventBus = buildEventBusFrom(infrastructure);
        IngesterBuilderFactory.Configuration configuration = IngesterBuilderFactory.watchdog(
                infrastructure.watchdog().url(), ingester.id());
        return Objects.isNull(ingester.flush()) ?
                buildReactiveIngesterFrom(ingester, infrastructure, configuration) :
                buildBufferedIngesterFrom(ingester, infrastructure, configuration);
    }

    private static Ingester buildBufferedIngesterFrom(IngesterDescriptor ingester, InfrastructureDescriptor infrastructure, IngesterBuilderFactory.Configuration configuration) {
        EventBus eventBus = buildEventBusFrom(infrastructure);
        return configuration.buffered()
                .from(eventBus)
                .flushPolicy(buildFlushPolicyFrom(ingester.flush()))
                .storesWith(DataLakeWriterFactory.from(infrastructure.dataLake()))
                .readWith(InboxReaderFactory.from(infrastructure.inbox()))
                .quarantineStore(System.out::println)
                .on(eventBus)
                .updatedWith(InboxUpdaterFactory.from(infrastructure.inbox()))
                .create();
    }

    private static Ingester buildReactiveIngesterFrom(IngesterDescriptor ingester, InfrastructureDescriptor infrastructure, IngesterBuilderFactory.Configuration configuration) {
        EventBus eventBus = buildEventBusFrom(infrastructure);
        return configuration.reactive()
                .from(eventBus)
                .storesWith(DataLakeWriterFactory.from(infrastructure.dataLake()))
                .readWith(InboxReaderFactory.from(infrastructure.inbox()))
                .quarantineStore(System.out::println)
                .on(eventBus)
                .updatedWith(InboxUpdaterFactory.from(infrastructure.inbox()))
                .create();
    }

    private static EventBus buildEventBusFrom(InfrastructureDescriptor infrastructure) {
        return EventBusFactory.from(infrastructure.eventBus());
    }

    private static FlushPolicy buildFlushPolicyFrom(FlushIngesterDescriptor flush) {
        return FlushPolicy.whenReaches(flush.size()).orEvery(Duration.ofSeconds(flush.every()));
    }
}
