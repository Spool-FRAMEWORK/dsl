package software.spool.dsl.compose;

import java.util.HashMap;
import java.util.Map;

/**
 * Which file, and which profile, set each path of a composed descriptor. A path nobody set takes the
 * origin of the closest path above it that was set, and the root descriptor when there is none.
 */
public final class DescriptorOrigins {
    private final Origin root;
    private final Map<String, Origin> byPath = new HashMap<>();

    DescriptorOrigins(Origin root) {
        this.root = root;
    }

    void record(String path, Origin origin) {
        byPath.put(path, origin);
    }

    public Origin originOf(String path) {
        for (String candidate = path; !candidate.isEmpty(); candidate = parentOf(candidate)) {
            Origin origin = byPath.get(candidate);
            if (origin != null) return origin;
        }
        return root;
    }

    private static String parentOf(String path) {
        return path.substring(0, Math.max(0, Math.max(path.lastIndexOf('.'), path.lastIndexOf('['))));
    }
}
