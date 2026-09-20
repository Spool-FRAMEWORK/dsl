package software.spool.dsl.reader;

import java.util.Optional;

/**
 * Turns a raw YAML value into the type a descriptor needs. A new kind of value is a new parser, the
 * {@link DescriptorReader} does not change.
 */
public interface ValueParser<T> {

    /** The parsed value, or empty if the raw value does not have the right shape. */
    Optional<T> parse(Object raw);

    /** Completes "must be ...", for example "an integer". */
    String expected();
}
