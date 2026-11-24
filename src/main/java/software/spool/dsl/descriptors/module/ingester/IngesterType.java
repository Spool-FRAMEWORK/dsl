package software.spool.dsl.descriptors.module.ingester;

/** How an ingester writes to the data lake: in batches, or each envelope on arrival. */
public enum IngesterType {
    FLUSH,
    REACTIVE
}
