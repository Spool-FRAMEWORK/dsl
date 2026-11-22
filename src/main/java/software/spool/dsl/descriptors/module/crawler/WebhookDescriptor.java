package software.spool.dsl.descriptors.module.crawler;

import java.util.Optional;

/**
 * Who may send deliveries to a WEBHOOK crawler (ADR-013): the authentication provider that identifies the sender and
 * the role the sender needs, and the largest body it accepts when the descriptor sets one. A missing value stops the
 * load when the crawler is built.
 */
public record WebhookDescriptor(Optional<String> authentication, Optional<String> role, Optional<Integer> maxBodySize) {
    public static WebhookDescriptor none() {
        return new WebhookDescriptor(Optional.empty(), Optional.empty(), Optional.empty());
    }
}
