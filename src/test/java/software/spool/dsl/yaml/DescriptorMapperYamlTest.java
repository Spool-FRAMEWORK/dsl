package software.spool.dsl.yaml;

import org.junit.jupiter.api.Test;
import software.spool.core.adapter.jackson.PayloadDeserializerFactory;
import software.spool.core.port.serde.EnrichmentRule;
import software.spool.core.port.serde.NamingConvention;
import software.spool.dsl.descriptors.SpoolNodeDescriptor;
import software.spool.dsl.descriptors.module.crawler.CrawlerDescriptor;
import software.spool.dsl.descriptors.module.ingester.IngesterDescriptor;
import software.spool.dsl.descriptors.module.janitor.JanitorDescriptor;
import software.spool.dsl.yaml.raw.RawSpoolNodeDescriptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads real YAML through the same deserializer the DSL uses, so the whole path is covered. */
class DescriptorMapperYamlTest {

    @Test
    void map_theSampleDescriptorShippedWithTheDsl_keepsEveryValue() throws IOException {
        byte[] yaml = getClass().getResourceAsStream("/Spool.yaml").readAllBytes();

        SpoolNodeDescriptor node = DescriptorMapper.map(read(yaml));

        assertThat(node.infrastructure().watchdog()).isEqualTo("http://host.docker.internal:8090");
        assertThat(node.infrastructure().eventBus().type()).isEqualTo("IN_MEMORY");
        assertThat(node.infrastructure().inbox().type()).isEqualTo("FILE_SYSTEM");
        assertThat(node.infrastructure().inbox().configuration()).containsEntry("path", "D:/spool/inbox");
        assertThat(node.infrastructure().dataLake().type()).isEqualTo("FILE_SYSTEM");
        assertThat(node.modules()).hasSize(3);

        CrawlerDescriptor crawler = (CrawlerDescriptor) node.modules().get(0);
        assertThat(crawler.type()).isEqualTo("POLL");
        assertThat(crawler.id()).isEqualTo("synthea-crawler");
        assertThat(crawler.source().type()).isEqualTo("HTTP");
        assertThat(crawler.source().mediaType()).isEqualTo("JSON_ARRAY");
        assertThat(crawler.source().rootPath()).isEqualTo("events");
        assertThat(crawler.source().configuration())
                .containsEntry("sourceId", "synthea-api")
                .containsEntry("scheduleMilliseconds", "5000");
        assertThat(crawler.eventMapping().namingConvention()).isEqualTo(NamingConvention.SNAKE_CASE);
        assertThat(crawler.eventMapping().domainMappingList()).isEmpty();
        assertThat(crawler.eventMapping().attributeList()).isEmpty();

        IngesterDescriptor ingester = (IngesterDescriptor) node.modules().get(1);
        assertThat(ingester.type()).isEqualTo("REACTIVE");
        assertThat(ingester.id()).isEqualTo("synthea-ingester");

        JanitorDescriptor janitor = (JanitorDescriptor) node.modules().get(2);
        assertThat(janitor.id()).isEqualTo("synthea-janitor");
        assertThat(janitor.configuration())
                .containsEntry("milliseconds", "1000")
                .containsEntry("millisecondsThreshold", "10000")
                .containsEntry("millisecondsTTL", "100000");
    }

    @Test
    void map_crawlerWithEnrichmentAndListsInBothForms_keepsThem() {
        String yaml = """
                infrastructure:
                  eventBus: {type: IN_MEMORY}
                  inbox: {type: FILE_SYSTEM}
                  dataLake: {type: FILE_SYSTEM}
                modules:
                  - crawler:
                      type: STREAM
                      id: c1
                      source:
                        type: KAFKA
                        mediaType: JSON_OBJECT
                        enrichment:
                          - source: a
                            target: b
                          - source: c
                      eventMapping:
                        namingConvention: CAMEL_CASE
                        attributeList:
                          - value: patientId
                          - encounterId
                        domainMappingList: [PatientEvent]
                """;

        CrawlerDescriptor crawler = (CrawlerDescriptor) DescriptorMapper.map(read(yaml.getBytes(StandardCharsets.UTF_8)))
                .modules().get(0);

        assertThat(crawler.source().rootPath()).isEmpty();
        assertThat(crawler.source().configuration()).isEmpty();
        assertThat(crawler.source().enrichment())
                .containsExactly(new EnrichmentRule("a", "b"), new EnrichmentRule("c", "c"));
        assertThat(crawler.eventMapping().attributeList()).containsExactly("patientId", "encounterId");
        assertThat(crawler.eventMapping().domainMappingList()).containsExactly("PatientEvent");
    }

    private static RawSpoolNodeDescriptor read(byte[] yaml) {
        return PayloadDeserializerFactory.yaml().as(RawSpoolNodeDescriptor.class).deserialize(yaml);
    }
}
