package software.spool.dsl.descriptors.module;

import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;

import java.time.Duration;
import java.util.Optional;

/**
 * When a module that works in rounds runs, from its {@code schedule:} block (ADR-015). Today it only says
 * {@code every}; the calendar forms of ADR-015 come with DEV-85.
 */
public record ScheduleDescriptor(Duration every) {
    private static final String SCHEDULE = "schedule";

    /** The schedule of a module, or nothing when it does not declare one. */
    public static Optional<ScheduleDescriptor> of(DescriptorReader module) {
        return module.value(SCHEDULE).map(declared -> {
            DescriptorReader schedule = module.object(SCHEDULE).knownKeysOnly(ScheduleDescriptor.class);
            return new ScheduleDescriptor(schedule.required("every", ValueParsers.DURATION));
        });
    }
}
