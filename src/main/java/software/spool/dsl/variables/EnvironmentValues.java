package software.spool.dsl.variables;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The values a descriptor read from the environment, so that no message shows them: each one is written as the
 * reference it came from, such as {@code ${env:TOKEN}} (ADR-011).
 */
public final class EnvironmentValues {
    private final Map<String, String> referenceByValue = new LinkedHashMap<>();

    void remember(String value, String reference) {
        if (!value.isEmpty()) referenceByValue.put(value, reference);
    }

    /** The text with every value read from the environment replaced by its reference. */
    public String mask(String text) {
        if (text == null) return null;
        String masked = text;
        for (Map.Entry<String, String> entry : referenceByValue.entrySet().stream()
                .sorted(Comparator.comparingInt((Map.Entry<String, String> e) -> e.getKey().length()).reversed())
                .toList()) {
            masked = masked.replace(entry.getKey(), entry.getValue());
        }
        return masked;
    }
}
