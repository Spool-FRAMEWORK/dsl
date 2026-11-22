package software.spool.dsl.descriptors.module;

import software.spool.core.adapter.schedule.CronUtilsCronCalendar;
import software.spool.core.model.schedule.CronExpression;
import software.spool.core.model.schedule.InvalidScheduleException;
import software.spool.core.model.schedule.Schedule;
import software.spool.core.port.schedule.CronCalendar;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParser;
import software.spool.dsl.reader.ValueParsers;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Reads the {@code schedule:} of a module that works in rounds (ADR-015): {@code once}, {@code every} with a
 * positive duration, or {@code cron} with the {@code zone} it is read in. Only one form at a time.
 */
public final class ScheduleReader {
    private static final String SCHEDULE = "schedule";
    private static final String ONCE = "once";
    private static final String EVERY = "every";
    private static final String CRON = "cron";
    private static final String ZONE = "zone";
    private static final String REBOOT = "@reboot";

    private static final ValueParser<ZoneId> TIME_ZONE = ValueParsers.of("a time zone such as Europe/Madrid or UTC",
            raw -> ValueParsers.STRING.parse(raw).flatMap(ScheduleReader::zoneNamed));

    private final ValueParser<CronExpression> cronExpression;

    public ScheduleReader(CronCalendar calendar) {
        this.cronExpression = ValueParsers.of("a Unix cron expression of 5 fields or a macro",
                raw -> ValueParsers.STRING.parse(raw).flatMap(text -> cronExpression(text, calendar)));
    }

    /** The reader with the cron calendar of the framework. */
    public static ScheduleReader standard() {
        return new ScheduleReader(new CronUtilsCronCalendar());
    }

    /** The schedule of a module, or nothing when it does not declare one. */
    public Optional<Schedule> read(DescriptorReader module) {
        return module.value(SCHEDULE).map(declared -> declared instanceof Map<?, ?>
                ? fromObject(module.object(SCHEDULE))
                : fromValue(module.path() + "." + SCHEDULE, declared));
    }

    private static Schedule fromValue(String path, Object declared) {
        if (ONCE.equals(declared)) return Schedule.once();
        throw new InvalidDescriptorException(path, "must be once or an object with every or cron, got '" + declared + "'");
    }

    private Schedule fromObject(DescriptorReader schedule) {
        schedule.knownKeysOnly(List.of(EVERY, CRON, ZONE));
        boolean every = schedule.value(EVERY).isPresent();
        boolean cron = schedule.value(CRON).isPresent();
        if (every && cron) throw new InvalidDescriptorException(schedule.path(), "must have either every or cron, not both");
        if (every) return every(schedule);
        if (cron) return cron(schedule);
        throw new InvalidDescriptorException(schedule.path(), "must have every or cron");
    }

    private static Schedule every(DescriptorReader schedule) {
        if (schedule.value(ZONE).isPresent()) {
            throw new InvalidDescriptorException(schedule.path() + "." + ZONE, "is only allowed with cron");
        }
        Duration interval = schedule.required(EVERY, ValueParsers.DURATION);
        if (!interval.isPositive()) {
            throw new InvalidDescriptorException(schedule.path() + "." + EVERY,
                    "must be a positive duration, got '" + schedule.value(EVERY).orElseThrow() + "'");
        }
        return Schedule.every(interval);
    }

    private Schedule cron(DescriptorReader schedule) {
        if (schedule.value(CRON).filter(ScheduleReader::isReboot).isPresent()) {
            throw new InvalidDescriptorException(schedule.path() + "." + CRON,
                    "does not accept " + REBOOT + ": use schedule: once to run once when the module starts");
        }
        return Schedule.cron(schedule.required(CRON, cronExpression), schedule.required(ZONE, TIME_ZONE));
    }

    private static boolean isReboot(Object expression) {
        return String.valueOf(expression).strip().toLowerCase(Locale.ROOT).equals(REBOOT);
    }

    private static Optional<CronExpression> cronExpression(String text, CronCalendar calendar) {
        try {
            return Optional.of(CronExpression.parse(text, calendar));
        } catch (InvalidScheduleException invalid) {
            return Optional.empty();
        }
    }

    private static Optional<ZoneId> zoneNamed(String name) {
        try {
            return Optional.of(ZoneId.of(name));
        } catch (DateTimeException unknown) {
            return Optional.empty();
        }
    }
}
