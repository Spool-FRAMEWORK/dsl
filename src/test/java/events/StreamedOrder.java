package events;

import software.spool.core.model.Event;

import java.time.Instant;

/** The event a stream crawler reads in the tests, in the package the DSL looks event classes up in. */
public record StreamedOrder(String payload) implements Event {
    @Override public String eventId() { return "streamed-order-id"; }
    @Override public String causationId() { return "streamed-order-causation-id"; }
    @Override public String correlationId() { return "streamed-order-correlation-id"; }
    @Override public Instant timestamp() { return Instant.EPOCH; }
}
