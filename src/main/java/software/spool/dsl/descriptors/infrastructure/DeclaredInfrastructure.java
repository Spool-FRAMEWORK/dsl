package software.spool.dsl.descriptors.infrastructure;

import software.spool.dsl.InvalidDescriptorException;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Every piece the descriptor declares in {@code infrastructure}, by name, in the order they are written. */
public final class DeclaredInfrastructure {
    private final Map<String, InfrastructureComponentDescriptor> pieces;

    public DeclaredInfrastructure(List<InfrastructureComponentDescriptor> pieces) {
        Map<String, InfrastructureComponentDescriptor> byName = new LinkedHashMap<>();
        pieces.forEach(piece -> byName.put(piece.name(), piece));
        this.pieces = Collections.unmodifiableMap(byName);
    }

    public static DeclaredInfrastructure of(InfrastructureComponentDescriptor... pieces) {
        return new DeclaredInfrastructure(List.of(pieces));
    }

    public List<InfrastructureComponentDescriptor> pieces() {
        return List.copyOf(pieces.values());
    }

    public Optional<InfrastructureComponentDescriptor> piece(String name) {
        return Optional.ofNullable(pieces.get(name));
    }

    public List<InfrastructureComponentDescriptor> ofKind(InfrastructureKind kind) {
        return pieces.values().stream().filter(piece -> piece.kind() == kind).toList();
    }

    /**
     * The pieces a module names in {@code uses}, by kind. Fails on a name that is not declared and on two pieces of
     * the same kind, with the path of {@code uses}.
     */
    public Map<InfrastructureKind, InfrastructureComponentDescriptor> named(String usesPath, List<String> uses) {
        Map<InfrastructureKind, InfrastructureComponentDescriptor> named = new EnumMap<>(InfrastructureKind.class);
        for (String name : uses) {
            InfrastructureComponentDescriptor piece = piece(name).orElseThrow(() -> new InvalidDescriptorException(usesPath,
                    "names '" + name + "', which is not declared in infrastructure. Declared: " + names(List.copyOf(pieces.values()))));
            InfrastructureComponentDescriptor other = named.putIfAbsent(piece.kind(), piece);
            if (other != null) {
                throw new InvalidDescriptorException(usesPath,
                        "names two " + piece.kind() + " pieces: " + other.name() + ", " + piece.name());
            }
        }
        return named;
    }

    static String names(List<InfrastructureComponentDescriptor> pieces) {
        if (pieces.isEmpty()) return "none";
        return String.join(", ", pieces.stream().map(InfrastructureComponentDescriptor::name).toList());
    }
}
