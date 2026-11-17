package software.spool.dsl.descriptors.module.janitor;

import software.spool.dsl.descriptors.module.ScheduleDescriptor;
import software.spool.dsl.descriptors.module.SpoolModuleDescriptor;

import java.time.Duration;
import java.util.Optional;

/** A janitor, with every value read once from the descriptor. What is missing keeps the janitor's default. */
public record JanitorDescriptor(
        String id,
        Optional<ScheduleDescriptor> schedule,
        Optional<Duration> threshold,
        Optional<Duration> ttl,
        Optional<Duration> quarantineTtl,
        Optional<Duration> quarantineRetention,
        Optional<Integer> maxInboxUpdateAttempts
) implements SpoolModuleDescriptor {

    /** A janitor with the defaults for everything but its id. */
    public static JanitorDescriptor withDefaults(String id) {
        return new JanitorDescriptor(id, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    @Override public String moduleType() {
        return "JANITOR";
    }
}
