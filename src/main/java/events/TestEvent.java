package events;

import software.spool.core.model.Event;

import java.time.Instant;

public record TestEvent(
        String eventId,
        String causationId,
        String correlationId,
        Instant timestamp
) implements Event { }
