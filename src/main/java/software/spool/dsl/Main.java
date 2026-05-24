package software.spool.dsl;

import events.TestEvent;
import events.TimeStampEvent;
import software.spool.core.port.bus.EventBus;
import software.spool.infrastructure.PluginResolver;
import software.spool.infrastructure.spi.provider.PluginConfiguration;
import software.spool.infrastructure.spi.provider.bus.EventBusProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

public class Main {
    public static void main(String[] args) throws IOException {
        EventBus bus = PluginResolver.get(EventBusProvider.class, "IN_MEMORY").create(PluginConfiguration.empty());
        bus.subscribe(TestEvent.class, System.out::println);
        SpoolNodeDSL.fromDescriptor("/Spool.yaml").start();
        bus.publish(
                new TestEvent(
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        Instant.now()
                )
        );
    }
}