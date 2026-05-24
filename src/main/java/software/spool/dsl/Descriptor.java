package software.spool.dsl;

import java.util.Map;

public interface Descriptor {
    String type();
    Map<String, String> configuration();
}