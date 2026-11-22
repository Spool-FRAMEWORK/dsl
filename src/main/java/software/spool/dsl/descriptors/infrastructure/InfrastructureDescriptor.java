package software.spool.dsl.descriptors.infrastructure;

import software.spool.core.port.health.PieceMonitor;
import software.spool.dsl.InvalidDescriptorException;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The infrastructure of one module. Each kind is resolved when the module asks for it: the piece the module names in
 * {@code uses}, or else the only piece of that kind. It remembers the kinds the module asked for, so that a piece
 * named in {@code uses} that the module never used can be reported.
 */
public final class InfrastructureDescriptor {
    private final Optional<String> watchdog;
    private final DeclaredInfrastructure declared;
    private final String modulePath;
    private final Map<InfrastructureKind, InfrastructureComponentDescriptor> named;
    private final Set<InfrastructureKind> asked = EnumSet.noneOf(InfrastructureKind.class);
    private final Map<String, PieceMonitor> monitors = new LinkedHashMap<>();

    public InfrastructureDescriptor(Optional<String> watchdog, DeclaredInfrastructure declared, String modulePath,
                                    Map<InfrastructureKind, InfrastructureComponentDescriptor> named) {
        this.watchdog = watchdog;
        this.declared = declared;
        this.modulePath = modulePath;
        this.named = Map.copyOf(named);
    }

    /** The infrastructure of a module that names no piece, so it uses the only piece of each kind. */
    public static InfrastructureDescriptor unnamed(Optional<String> watchdog, DeclaredInfrastructure declared) {
        return new InfrastructureDescriptor(watchdog, declared, "the module", Map.of());
    }

    /** The URL of the watchdog, or {@code null} when the node sends no heartbeats, as the module builders expect. */
    public String watchdog() {
        return watchdog.orElse(null);
    }

    public InfrastructureComponentDescriptor eventBus() {
        return required(InfrastructureKind.EVENT_BUS);
    }

    public InfrastructureComponentDescriptor inbox() {
        return required(InfrastructureKind.INBOX);
    }

    public InfrastructureComponentDescriptor dataLake() {
        return required(InfrastructureKind.DATA_LAKE);
    }

    public InfrastructureComponentDescriptor dataMart() {
        return required(InfrastructureKind.DATA_MART);
    }

    public InfrastructureComponentDescriptor quarantineRepository() {
        return required(InfrastructureKind.QUARANTINE_REPOSITORY);
    }

    /** The piece that keeps the metadata of the aggregates, which a mount may go without. */
    public Optional<InfrastructureComponentDescriptor> aggregateMetadata() {
        return find(InfrastructureKind.AGGREGATE_METADATA);
    }

    /** The piece that keeps the headers of the envelopes of the data lake, which the data lake may go without. */
    public Optional<InfrastructureComponentDescriptor> envelopeHeaders() {
        return find(InfrastructureKind.ENVELOPE_HEADERS);
    }

    /** Whether the module names a piece of the kind in {@code uses}. It does not count as asking for the piece. */
    public boolean names(InfrastructureKind kind) {
        return named.containsKey(kind);
    }

    /** The piece of a kind the module may go without: nothing when none is declared. */
    public Optional<InfrastructureComponentDescriptor> find(InfrastructureKind kind) {
        asked.add(kind);
        if (named.containsKey(kind)) return Optional.of(named.get(kind));
        List<InfrastructureComponentDescriptor> candidates = declared.ofKind(kind);
        if (candidates.size() > 1) {
            throw new InvalidDescriptorException(modulePath, "needs " + article(kind) + " " + kind
                    + ": name one in uses. Declared: " + DeclaredInfrastructure.names(candidates));
        }
        return candidates.stream().findFirst();
    }

    /** The kinds the module has asked for so far. */
    /** The monitor of a piece the module uses, made with the given function the first time the piece is asked for. */
    public PieceMonitor monitorOf(InfrastructureComponentDescriptor piece,
                                  Function<InfrastructureComponentDescriptor, PieceMonitor> monitor) {
        return monitors.computeIfAbsent(piece.name(), name -> monitor.apply(piece));
    }

    /** The monitors of the pieces the module asked for, in the order it asked for them. */
    public List<PieceMonitor> monitors() {
        return List.copyOf(monitors.values());
    }

    public Set<InfrastructureKind> askedKinds() {
        return Set.copyOf(asked);
    }

    /** Fails on the first piece named in {@code uses} whose kind the module never asked for. */
    public void checkEveryNamedPieceIsUsed() {
        named.values().stream()
                .filter(piece -> !asked.contains(piece.kind()))
                .findFirst()
                .ifPresent(piece -> {
                    throw new InvalidDescriptorException(modulePath + ".uses",
                            "names '" + piece.name() + "', " + article(piece.kind()) + " " + piece.kind()
                                    + " the module does not use");
                });
    }

    private InfrastructureComponentDescriptor required(InfrastructureKind kind) {
        return find(kind).orElseThrow(() -> new InvalidDescriptorException(modulePath,
                "needs " + article(kind) + " " + kind + ", and infrastructure declares none"));
    }

    private static String article(InfrastructureKind kind) {
        return "AEIOU".indexOf(kind.name().charAt(0)) >= 0 ? "an" : "a";
    }
}
