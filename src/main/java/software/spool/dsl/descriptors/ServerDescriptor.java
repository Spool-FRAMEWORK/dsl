package software.spool.dsl.descriptors;

/** The HTTP server of a node: the port it opens, 8080 when the descriptor does not say it. */
public record ServerDescriptor(int port) {
    public static final int DEFAULT_PORT = 8080;

    public static ServerDescriptor defaults() {
        return new ServerDescriptor(DEFAULT_PORT);
    }
}
