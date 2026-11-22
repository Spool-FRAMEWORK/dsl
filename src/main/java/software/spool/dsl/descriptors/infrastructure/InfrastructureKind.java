package software.spool.dsl.descriptors.infrastructure;

/** What a piece of the infrastructure is. It tells the DSL which factory builds the piece; its type picks the plugin. */
public enum InfrastructureKind {
    EVENT_BUS,
    INBOX,
    DATA_LAKE,
    DATA_MART,
    QUARANTINE_REPOSITORY,
    AGGREGATE_METADATA,
    ENVELOPE_HEADERS
}
