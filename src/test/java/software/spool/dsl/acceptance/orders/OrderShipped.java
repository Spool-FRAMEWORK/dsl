package software.spool.dsl.acceptance.orders;

import software.spool.core.model.Event;

import java.time.Instant;

/** An event outside the package the DSL looks event classes up in by default. */
public record OrderShipped(String payload) implements Event {
    @Override public String eventId() { return "order-shipped-id"; }
    @Override public String causationId() { return "order-shipped-causation-id"; }
    @Override public String correlationId() { return "order-shipped-correlation-id"; }
    @Override public Instant timestamp() { return Instant.EPOCH; }
}
