package software.spool.dsl.yaml;

import events.StreamedOrder;
import org.junit.jupiter.api.Test;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.core.port.serde.NamingConvention;
import software.spool.core.utils.media.MediaTypes;
import software.spool.dsl.descriptors.infrastructure.InfrastructureComponentDescriptor;
import software.spool.dsl.descriptors.infrastructure.InfrastructureKind;
import software.spool.dsl.InvalidDescriptorException;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.core.model.schedule.Schedule;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.io.IOException;
import java.time.Duration;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Reads real YAML through the same deserializer the DSL uses, so the whole path is covered. */
class DescriptorMapperYamlTest {

    @Test
    void map_theSampleDescriptor_keepsEveryValue() throws IOException {
        byte[] yaml = getClass().getResourceAsStream("/descriptors/sample-node.yaml").readAllBytes();

        SpoolNodeDescriptor node = DescriptorMapper.map(read(yaml));

        assertThat(node.watchdog()).contains("http://host.docker.internal:8090");
        assertThat(node.infrastructure().piece("eventBus").orElseThrow().type()).isEqualTo("IN_MEMORY");
        assertThat(node.infrastructure().piece("inbox").orElseThrow().type()).isEqualTo("FILE_SYSTEM");
        assertThat(node.infrastructure().piece("inbox").orElseThrow().configuration()).containsEntry("path", "D:/spool/inbox");
        assertThat(node.infrastructure().piece("dataLake").orElseThrow().type()).isEqualTo("FILE_SYSTEM");
        assertThat(node.modules()).hasSize(3);

        CrawlerDescriptor crawler = (CrawlerDescriptor) node.modules().get(0);
        assertThat(crawler.type()).isEqualTo("POLL");
        assertThat(crawler.id()).isEqualTo("synthea-crawler");
        assertThat(crawler.source().type()).isEqualTo("HTTP");
        assertThat(crawler.source().mediaType()).isEqualTo(MediaTypes.JSON);
        assertThat(crawler.source().rootPath()).isEqualTo("events");
        assertThat(crawler.source().configuration()).containsEntry("sourceId", "synthea-api");
        assertThat(crawler.schedule()).isEqualTo(Schedule.every(Duration.ofSeconds(5)));
        assertThat(crawler.eventMapping().namingConvention()).isEqualTo(NamingConvention.SNAKE_CASE);
        assertThat(crawler.eventMapping().domainEvents()).isEmpty();
        assertThat(crawler.eventMapping().attributeList()).isEmpty();

        IngesterDescriptor ingester = (IngesterDescriptor) node.modules().get(1);
        assertThat(ingester.type()).isEqualTo("REACTIVE");
        assertThat(ingester.id()).isEqualTo("synthea-ingester");

        JanitorDescriptor janitor = (JanitorDescriptor) node.modules().get(2);
        assertThat(janitor.id()).isEqualTo("synthea-janitor");
        assertThat(janitor.schedule()).contains(Schedule.every(Duration.ofSeconds(1)));
        assertThat(janitor.threshold()).contains(Duration.ofSeconds(10));
        assertThat(janitor.ttl()).contains(Duration.ofSeconds(100));
        assertThat(janitor.quarantineRetention()).contains(Duration.ofDays(30));
    }

    @Test
    void map_crawlerWithEnrichmentAndListsInBothForms_keepsThem() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  inbox: {kind: INBOX, type: FILE_SYSTEM}
                  dataLake: {kind: DATA_LAKE, type: FILE_SYSTEM}
                modules:
                  - crawler:
                      type: STREAM
                      id: c1
                      source:
                        type: KAFKA
                        normalizer: JSON_OBJECT
                        mediaType: JSON
                        enrichment:
                          - source: a
                            target: b
                          - source: c
                      eventMapping:
                        namingConvention: CAMEL_CASE
                        attributeList:
                          - value: patientId
                          - encounterId
                        domainMappingList: [StreamedOrder]
                """;

        CrawlerDescriptor crawler = (CrawlerDescriptor) DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8)))
                .modules().get(0);

        assertThat(crawler.source().rootPath()).isEmpty();
        assertThat(crawler.source().configuration()).isEmpty();
        assertThat(crawler.source().enrichment())
                .containsExactly(new EnrichmentRule("a", "b"), new EnrichmentRule("c", "c"));
        assertThat(crawler.eventMapping().attributeList()).containsExactly("patientId", "encounterId");
        assertThat(crawler.eventMapping().domainEvents()).containsExactly(StreamedOrder.class);
    }

    @Test
    void map_quarantineRepository_keepsItsTypeAndConfiguration() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  quarantineRepository:
                    kind: QUARANTINE_REPOSITORY
                    type: FILE_SYSTEM
                    configuration:
                      path: "/spool/quarantine"
                modules:
                  - janitor:
                      id: j1
                """;

        InfrastructureComponentDescriptor quarantine = DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8)))
                .infrastructure().piece("quarantineRepository").orElseThrow();

        assertThat(quarantine.kind()).isEqualTo(InfrastructureKind.QUARANTINE_REPOSITORY);
        assertThat(quarantine.pluginName()).isEqualTo("FILE_SYSTEM");
        assertThat(quarantine.configuration()).containsEntry("path", "/spool/quarantine");
    }

    @Test
    void map_quarantineStoreWithoutAKind_saysWhichKindToAdd() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  quarantineStore:
                    type: FILE_SYSTEM
                modules:
                  - janitor:
                      id: j1
                """;

        assertThatThrownBy(() -> DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessage("infrastructure.quarantineStore needs a kind: add kind: QUARANTINE_REPOSITORY");
    }

    @Test
    void map_janitorWithAQuarantineTtl_keepsIt() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  inbox: {kind: INBOX, type: FILE_SYSTEM}
                modules:
                  - janitor:
                      id: j1
                      configuration:
                        ttl: 1d
                        quarantineTtl: 7d
                """;

        JanitorDescriptor janitor = (JanitorDescriptor) DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8)))
                .modules().get(0);

        assertThat(janitor.ttl()).contains(Duration.ofDays(1));
        assertThat(janitor.quarantineTtl()).contains(Duration.ofDays(7));
    }

    @Test
    void map_janitorWithAQuarantineTtlThatIsNotADuration_namesTheKey() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  inbox: {kind: INBOX, type: FILE_SYSTEM}
                modules:
                  - janitor:
                      id: j1
                      configuration:
                        quarantineTtl: soon
                """;

        assertThatThrownBy(() -> DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("quarantineTtl must be a duration such as 30d or P30D, got 'soon'");
    }

    @Test
    void map_janitorWithMaxInboxUpdateAttempts_keepsIt() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  inbox: {kind: INBOX, type: FILE_SYSTEM}
                modules:
                  - janitor:
                      id: j1
                      configuration:
                        maxInboxUpdateAttempts: 5
                """;

        JanitorDescriptor janitor = (JanitorDescriptor) DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8)))
                .modules().get(0);

        assertThat(janitor.maxInboxUpdateAttempts()).contains(5);
    }

    @Test
    void map_janitorWithMaxInboxUpdateAttemptsThatIsNotAnInteger_namesTheKey() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  inbox: {kind: INBOX, type: FILE_SYSTEM}
                modules:
                  - janitor:
                      id: j1
                      configuration:
                        maxInboxUpdateAttempts: many
                """;

        assertThatThrownBy(() -> DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(InvalidDescriptorException.class)
                .hasMessageContaining("maxInboxUpdateAttempts must be an integer, got 'many'");
    }

    @Test
    void map_janitorWithoutAQuarantineTtl_isValid() {
        String yaml = """
                infrastructure:
                  eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
                  inbox: {kind: INBOX, type: FILE_SYSTEM}
                modules:
                  - janitor:
                      id: j1
                """;

        JanitorDescriptor janitor = (JanitorDescriptor) DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8)))
                .modules().get(0);

        assertThat(janitor.quarantineTtl()).isEmpty();
    }

    private static RawSpoolNodeDescriptor read(byte[] yaml) {
        return RawSpoolNodeDescriptor.from(new YamlTreeParser().deserialize(yaml));
    }
}
