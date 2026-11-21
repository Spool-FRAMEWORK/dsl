package software.spool.dsl.yaml.raw;

import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.reader.DescriptorReader;
import software.spool.dsl.reader.ValueParsers;

import java.util.Map;
import java.util.Optional;

/** A piece of {@code infrastructure} as it is written: its kind may be missing in a descriptor of the old format. */
public record RawPieceDescriptor(
        Optional<InfrastructureKind> kind,
        String type,
        Map<String, String> configuration
) {
    static RawPieceDescriptor from(DescriptorReader piece) {
        piece.knownKeysOnly(RawPieceDescriptor.class);
        return new RawPieceDescriptor(
                piece.optional("kind", ValueParsers.oneOf(InfrastructureKind.class)),
                piece.optional("type", ValueParsers.STRING).orElse(null),
                piece.stringMap("configuration")
        );
    }
}
