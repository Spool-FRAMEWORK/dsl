package software.spool.dsl.variables;

import software.spool.dsl.InvalidDescriptorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Replaces the references to variables in the values of a composed descriptor, as ADR-011 decides.
 *
 * <ul>
 *     <li>{@code ${name}} is a variable of the {@value #VARIABLES} block, which may use other variables.</li>
 *     <li>{@code ${env:NAME}} is an environment variable, and {@code ${env:NAME:-default}} gives it a default.</li>
 *     <li>A doubled dollar before a reference writes it as plain text: {@code $${name}} stays {@code ${name}}.</li>
 * </ul>
 *
 * <p>
 * A reference works as a whole value or inside a text. Only values are replaced, never keys, and values that
 * are not text are kept as they are. Every variable is checked, even one nobody uses. The {@value #VARIABLES}
 * block does not reach the result. Each error names the path of the value and the reference, never the value
 * read from the environment.
 * </p>
 */
public final class DescriptorVariables {
    public static final String VARIABLES = "variables";
    private static final String ENVIRONMENT_PREFIX = "env:";
    private static final String DEFAULT_SEPARATOR = ":-";

    private final UnaryOperator<String> environment;
    private final EnvironmentValues environmentValues;
    private final Map<String, Object> declared = new LinkedHashMap<>();
    private final Map<String, String> resolved = new HashMap<>();

    /**
     * @param environment       reads an environment variable, giving {@code null} when it is not set
     * @param environmentValues collects the values read from the environment, to keep them out of messages
     */
    public DescriptorVariables(UnaryOperator<String> environment, EnvironmentValues environmentValues) {
        this.environment = environment;
        this.environmentValues = environmentValues;
    }

    /** The descriptor with every reference replaced and without its {@value #VARIABLES} block. */
    public Map<String, Object> resolve(Map<String, Object> tree) {
        Map<String, Object> withoutVariables = new LinkedHashMap<>(tree);
        declare(withoutVariables.remove(VARIABLES));
        for (String name : declared.keySet()) valueOf(name, new ArrayList<>());
        return replaceIn(withoutVariables, "");
    }

    private void declare(Object block) {
        if (block == null) return;
        if (!(block instanceof Map<?, ?> variables)) {
            throw new InvalidDescriptorException(VARIABLES, "must be an object of single values, got '" + block + "'");
        }
        variables.forEach((rawName, value) -> {
            String name = String.valueOf(rawName);
            if (value == null || value instanceof Map<?, ?> || value instanceof List<?>) {
                throw new InvalidDescriptorException(VARIABLES + "." + name, "must be a single value, got '" + value + "'");
            }
            declared.put(name, value);
        });
    }

    private String valueOf(String name, List<String> chain) {
        String known = resolved.get(name);
        if (known != null) return known;
        String path = VARIABLES + "." + name;
        if (chain.contains(name)) {
            List<String> cycle = new ArrayList<>(chain.subList(chain.indexOf(name), chain.size()));
            cycle.add(name);
            throw new InvalidDescriptorException(VARIABLES + "." + chain.get(chain.size() - 1),
                    "forms a cycle between variables: " + String.join(" -> ", cycle));
        }
        chain.add(name);
        String value = replaceIn(String.valueOf(declared.get(name)), path, chain);
        chain.remove(chain.size() - 1);
        resolved.put(name, value);
        return value;
    }

    @SuppressWarnings("unchecked")
    private <T> T replaceIn(T value, String path) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, nested) -> {
                String name = String.valueOf(key);
                copy.put(name, replaceIn(nested, path.isEmpty() ? name : path + "." + name));
            });
            return (T) copy;
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>();
            for (int i = 0; i < list.size(); i++) copy.add(replaceIn(list.get(i), path + "[" + i + "]"));
            return (T) copy;
        }
        if (value instanceof String text) return (T) replaceIn(text, path, new ArrayList<>());
        return value;
    }

    private String replaceIn(String text, String path, List<String> chain) {
        StringBuilder result = new StringBuilder();
        int index = 0;
        while (index < text.length()) {
            if (text.startsWith("$${", index)) {
                result.append("${");
                index += 3;
            } else if (text.startsWith("${", index)) {
                int end = text.indexOf('}', index + 2);
                if (end < 0) {
                    throw new InvalidDescriptorException(path, "has a reference that is not closed: '" + text.substring(index) + "'");
                }
                result.append(valueOfReference(text.substring(index + 2, end), path, chain));
                index = end + 1;
            } else {
                result.append(text.charAt(index));
                index++;
            }
        }
        return result.toString();
    }

    private String valueOfReference(String reference, String path, List<String> chain) {
        if (reference.startsWith(ENVIRONMENT_PREFIX)) return environmentValueOf(reference.substring(ENVIRONMENT_PREFIX.length()), path);
        if (!declared.containsKey(reference)) {
            throw new InvalidDescriptorException(path, "uses ${" + reference + "}, which is not declared in " + VARIABLES);
        }
        return valueOf(reference, chain);
    }

    private String environmentValueOf(String reference, String path) {
        int separator = reference.indexOf(DEFAULT_SEPARATOR);
        String name = separator < 0 ? reference : reference.substring(0, separator);
        Optional<String> fallback = separator < 0 ? Optional.empty() : Optional.of(reference.substring(separator + DEFAULT_SEPARATOR.length()));
        if (name.isBlank()) throw new InvalidDescriptorException(path, "uses ${env:" + reference + "}, which names no environment variable");
        String value = environment.apply(name);
        if (value != null && !value.isEmpty()) {
            environmentValues.remember(value, "${env:" + name + "}");
            return value;
        }
        return fallback.orElseThrow(() -> new InvalidDescriptorException(path,
                "uses ${env:" + name + "}, which is not set and has no default"));
    }
}
