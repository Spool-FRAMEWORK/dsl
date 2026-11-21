package software.spool.dsl.descriptors.module.janitor;

import java.util.Optional;

/**
 * The quarantine API of a janitor (ADR-014): the authentication provider that identifies its callers, the role each
 * kind of operation needs, and the size of its pages when the descriptor sets them.
 */
public record QuarantineServerDescriptor(
        String authentication,
        String readRole,
        String resolveRole,
        String bypassRole,
        Optional<Integer> pageSize,
        Optional<Integer> maxPageSize
) {
}
