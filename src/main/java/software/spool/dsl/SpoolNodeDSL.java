package software.spool.dsl;

import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.model.spool.SpoolModule;
import software.spool.core.model.spool.SpoolNode;
import software.spool.core.port.bus.EventBus;
import software.spool.core.port.serde.NamingConvention;
import software.spool.core.utils.polling.PollingConfiguration;
import software.spool.crawler.Main;
import software.spool.crawler.api.builder.CrawlerBuilderFactory;
import software.spool.crawler.api.builder.EventMappingSpecification;
import software.spool.crawler.api.port.source.PollSource;
import software.spool.crawler.api.utils.CrawlerPorts;
import software.spool.crawler.api.utils.StandardFormat;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.crawler.EventMappingDescriptor;
import software.spool.dsl.descriptors.module.crawler.PartitionAttributeDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.SourceDescriptor;
import software.spool.dsl.descriptors.module.crawler.source.poll.ScheduleDescriptor;
import software.spool.dsl.descriptors.module.feeder.FeederDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.feeder.api.builder.FeederBuilderFactory;
import software.spool.ingester.api.builder.IngesterBuilderFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public abstract class SpoolNodeDSL {
    public static void fromDescriptor(String path) throws IOException {
        try(BufferedInputStream is = new BufferedInputStream(
                Objects.requireNonNull(Main.class.getResourceAsStream(path)))) {
            fromDescriptor(PayloadDeserializerFactory.yaml().as(SpoolNodeDescriptor.class)
                    .deserialize(new String(is.readAllBytes())));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static void fromDescriptor(SpoolNodeDescriptor descriptor) throws IOException {
        SpoolNode node = SpoolNode.create();
        buildModulesFrom(descriptor.spoolModuleList(), descriptor.infrastructure())
                .forEach(node::register);
        node.start();
    }

    private static List<SpoolModule> buildModulesFrom(
            List<SpoolModuleDescriptor> moduleDescriptors,
            InfrastructureDescriptor infrastructure
    ) {
        return moduleDescriptors.stream()
                .map(m -> buildModuleFrom(m, infrastructure))
                .toList();
    }

    private static SpoolModule buildModuleFrom(
            SpoolModuleDescriptor moduleDescriptor,
            InfrastructureDescriptor infrastructure
    ) {
        return switch (moduleDescriptor.type()) {
            case CRAWLER -> buildCrawlerFrom(moduleDescriptor.crawler(), infrastructure);
            case FEEDER -> buildFeederFrom(moduleDescriptor.feeder(), infrastructure);
            case INGESTER -> buildIngesterFrom(moduleDescriptor.ingester(), infrastructure);
        };
    }

    private static SpoolModule buildIngesterFrom(IngesterDescriptor feeder, InfrastructureDescriptor infrastructure) {
        EventBus eventBus = EventBusFactory.from(infrastructure.eventBus());
        return IngesterBuilderFactory.watchdog(infrastructure.watchdog().url(), feeder.id())
                .buffered()
                .from(eventBus)
                .storesWith(DataLakeWriterFactory.from(infrastructure.dataLake()))
                .quarantineStore(System.out::println)
                .on(eventBus)
                .updatedWith(InboxUpdaterFactory.from(infrastructure.inbox()))
                .create();
    }

    private static SpoolModule buildFeederFrom(FeederDescriptor feeder, InfrastructureDescriptor infrastructure) {
        return FeederBuilderFactory.watchdog(infrastructure.watchdog().url(), feeder.id())
                .polling()
                .every(Duration.ofSeconds(feeder.poll().every()))
                .from(InboxReaderFactory.from(infrastructure.inbox()))
                .on(EventBusFactory.from(infrastructure.eventBus()))
                .with(InboxUpdaterFactory.from(infrastructure.inbox()))
                .create();
    }

    private static SpoolModule buildCrawlerFrom(
            CrawlerDescriptor crawler,
            InfrastructureDescriptor infrastructure
    ) {
        return CrawlerBuilderFactory.watchdog(infrastructure.watchdog().url(), crawler.id())
                .poll(buildPollSourceFrom(crawler.source()))
                .schedule(buildScheduleFrom(crawler.source().poll().schedule()))
                .ports(buildPortsFrom(infrastructure))
                .eventMapping(buildEventMappingFrom(crawler.eventMapping()))
                .createWith(StandardFormat.valueOf(crawler.source().format()));
    }

    private static PollSource<?> buildPollSourceFrom(SourceDescriptor sourceDescriptor) {
        return SourceFactory.pollFrom(sourceDescriptor);
    }

    private static PollingConfiguration buildScheduleFrom(ScheduleDescriptor scheduleDescriptor) {
        return PollingConfiguration.every(Duration.ofSeconds(scheduleDescriptor.every()));
    }

    private static EventMappingSpecification buildEventMappingFrom(EventMappingDescriptor eventMappingDescriptor) {
        return new EventMappingSpecification(NamingConvention.SNAKE_CASE)
                .addPartitionAttributes(eventMappingDescriptor.attributeList().stream()
                        .map(PartitionAttributeDescriptor::value)
                        .toArray(String[]::new));
    }

    private static CrawlerPorts buildPortsFrom(InfrastructureDescriptor infrastructure) {
        return CrawlerPorts.builder()
                .bus(EventBusFactory.from(infrastructure.eventBus()))
                .inbox(InboxWriterFactory.from(infrastructure.inbox()))
                .build();
    }
}
