package software.spool.dsl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The nodes of one process open their HTTP servers side by side, so no two descriptors can share a port. */
public final class DistinctPorts {
    private DistinctPorts() {}

    /** Fails on the first port two descriptors share, naming both. */
    public static void check(List<NamedDescriptor> descriptors) throws IOException {
        Map<Integer, String> owners = new HashMap<>();
        for (NamedDescriptor named : descriptors) {
            int port = named.descriptor().server().port();
            String owner = owners.putIfAbsent(port, named.name());
            if (owner != null) {
                throw new IOException(owner + " and " + named.name() + " both open port " + port
                        + ": give each node its own server.port");
            }
        }
    }
}
