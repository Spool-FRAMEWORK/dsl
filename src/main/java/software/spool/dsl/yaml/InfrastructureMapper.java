package software.spool.dsl.yaml;

import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.infrastructure.DeclaredInfrastructure;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.reader.CircuitPolicyReader;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.yaml.raw.RawInfrastructureDescriptor;
import software.spool.dsl.yaml.raw.RawPieceDescriptor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Turns the pieces of {@code infrastructure} into named pieces, each with its kind. */
final class InfrastructureMapper {
    private static final String INFRASTRUCTURE = "infrastructure";

    /** The keys of the format before named pieces, which a piece keeps as its name and only lacks its kind. */
    private static final Map<String, InfrastructureKind> FORMER_KEYS = Map.of(
            "eventBus", InfrastructureKind.EVENT_BUS,
            "inbox", InfrastructureKind.INBOX,
            "dataLake", InfrastructureKind.DATA_LAKE,
            "dataMart", InfrastructureKind.DATA_MART,
            "quarantineRepository", InfrastructureKind.QUARANTINE_REPOSITORY,
            "quarantineStore", InfrastructureKind.QUARANTINE_REPOSITORY);

    private InfrastructureMapper() {}

    static DeclaredInfrastructure map(RawInfrastructureDescriptor raw) {
        DescriptorReader.require(INFRASTRUCTURE, raw);
        List<InfrastructureComponentDescriptor> pieces = raw.pieces().entrySet().stream()
                .map(entry -> piece(entry.getKey(), entry.getValue()))
                .toList();
        return new DeclaredInfrastructure(pieces);
    }

    private static InfrastructureComponentDescriptor piece(String name, RawPieceDescriptor raw) {
        String path = INFRASTRUCTURE + "." + name;
        InfrastructureKind kind = raw.kind().orElseThrow(() -> missingKind(path, name));
        Map<String, String> configuration = raw.configuration() != null ? raw.configuration() : Map.of();
        return new InfrastructureComponentDescriptor(name, kind, DescriptorReader.require(path + ".type", raw.type()),
                configuration, CircuitPolicyReader.read(DescriptorReader.of(path + ".configuration", configuration)));
    }

    private static InvalidDescriptorException missingKind(String path, String name) {
        InfrastructureKind former = FORMER_KEYS.get(name);
        if (former != null) return new InvalidDescriptorException(path, "needs a kind: add kind: " + former);
        String kinds = Arrays.stream(InfrastructureKind.values()).map(Enum::name).collect(Collectors.joining(", "));
        return new InvalidDescriptorException(path + ".kind", "is required: one of " + kinds);
    }
}
