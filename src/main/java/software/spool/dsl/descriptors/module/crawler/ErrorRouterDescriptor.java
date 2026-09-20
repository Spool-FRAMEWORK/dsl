package software.spool.dsl.descriptors.module.crawler;

import java.util.Map;

/**
 * The error router a crawler sends its errors to: the name of an {@code ErrorRouterProvider} plugin and the
 * configuration it is given.
 */
public record ErrorRouterDescriptor(String type, Map<String, String> configuration) {
}
