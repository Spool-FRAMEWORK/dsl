package events;

import software.spool.core.model.Event;

import java.time.Instant;

public record SyntheaReceived(
        String PATIENT,
        String CATEGORY,
        String _type
) implements Event {
    @Override
    public String eventId() {
        return "";
    }

    @Override
    public String causationId() {
        return "";
    }

    @Override
    public String correlationId() {
        return "";
    }

    @Override
    public Instant timestamp() {
        return null;
    }
}
