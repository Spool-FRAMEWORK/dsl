package software.spool.dsl.yaml.raw;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.ServerDescriptor;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;

/** The {@code server} block, read into the server of the node. */
public record RawServerDescriptor(Integer port) {
    private static final int HIGHEST_PORT = 65535;

    static ServerDescriptor from(DescriptorReader server) {
        server.knownKeysOnly(RawServerDescriptor.class);
        return server.optional("port", ValueParsers.INTEGER)
                .map(port -> {
                    if (port < 1 || port > HIGHEST_PORT) {
                        throw new InvalidDescriptorException(server.path() + ".port",
                                "must be between 1 and " + HIGHEST_PORT + ", got '" + port + "'");
                    }
                    return new ServerDescriptor(port);
                })
                .orElseGet(ServerDescriptor::defaults);
    }
}
