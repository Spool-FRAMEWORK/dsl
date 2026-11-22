package software.spool.dsl.registry;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.spool.dsl.descriptors.DeclaredModule;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureDescriptor;
import software.spool.dsl.providers.authentication.RequestAuthenticators;
import software.spool.dsl.yaml.DescriptorMapper;
import software.spool.dsl.yaml.YamlTreeParser;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What every module provider declares in {@code uses} is what building its module asks for, so the role generated for
 * a module cannot drift from what the module does.
 */
class ModuleUsesTest {
    @TempDir
    Path folder;

    @ParameterizedTest
    @ValueSource(strings = {"poll-crawler", "stream-crawler", "webhook-crawler", "ingester", "janitor", "silver-mounter", "gold-mounter"})
    void uses_isWhatBuildingTheModuleAsksFor(String id) {
        SpoolNodeDescriptor node = node();
        DeclaredModule module = node.declaredModules().stream()
                .filter(declared -> declared.descriptor().id().equals(id)).findFirst().orElseThrow();
        InfrastructureDescriptor infrastructure = node.infrastructureOf(module);

        ModuleProviderRegistry.build(module.descriptor(), infrastructure, RequestAuthenticators.from(node.authentication()));

        assertThat(ModuleProviderRegistry.uses(module.descriptor())).isEqualTo(infrastructure.askedKinds());
    }

    private SpoolNodeDescriptor node() {
        String root = folder.toString().replace('\\', '/');
        String yaml = """
                authentication:
                  senders:
                    type: HMAC_SIGNATURE
                    configuration: {header: X-Signature-256, secret: a-shared-secret, roles: sender}
                infrastructure:
                  bus: {kind: EVENT_BUS, type: IN_MEMORY}
                  inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "%1$s/inbox"}}
                  lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: "%1$s/lake"}}
                  mart: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: "%1$s/mart"}}
                  quarantine: {kind: QUARANTINE_REPOSITORY, type: FILE_SYSTEM, configuration: {path: "%1$s/quarantine"}}
                modules:
                  - crawler:
                      type: POLL
                      id: poll-crawler
                      source: {type: HTTP, normalizer: JSON_ARRAY, mediaType: JSON, configuration: {sourceId: s, url: "http://localhost:1/events"}}
                      eventMapping: {namingConvention: SNAKE_CASE}
                  - crawler:
                      type: STREAM
                      id: stream-crawler
                      source: {type: IN_MEMORY, normalizer: JSON_ARRAY, mediaType: JSON, configuration: {sourceId: s, eventClassName: StreamedOrder}}
                      eventMapping: {namingConvention: SNAKE_CASE}
                  - crawler:
                      type: WEBHOOK
                      id: webhook-crawler
                      source: {type: WEBHOOK, normalizer: JSON_ARRAY, mediaType: JSON, configuration: {sourceId: s, path: /orders}}
                      eventMapping: {namingConvention: SNAKE_CASE}
                      webhook: {authentication: senders, role: sender}
                  - ingester:
                      type: REACTIVE
                      id: ingester
                  - janitor:
                      id: janitor
                  - mounter:
                      id: silver-mounter
                      type: POLL
                      schedule: {every: 1m}
                      mode: TRANSFORMATION
                      read: {partitions: ["source=payments"]}
                      write: {dataMart: payments, partitioning: {type: SAME_AS_SOURCE}}
                  - mounter:
                      id: gold-mounter
                      type: REACTIVE
                      mode: AGGREGATION
                      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
                      read: {partitions: ["source=payments"]}
                      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
                """.formatted(root);
        return DescriptorMapper.map(RawSpoolNodeDescriptor.from(
                new YamlTreeParser().deserialize(yaml.getBytes(StandardCharsets.UTF_8))));
    }
}
