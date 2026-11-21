package software.spool.dsl.acceptance.normalizers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.yaml.snakeyaml.Yaml;
import software.spool.core.pipeline.Step;
import software.spool.crawler.api.normalizer.NormalizerStep;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** The steps the scenarios compose normalizers with, declared the way an application declares its own. */
public final class DeclaredNormalizerSteps {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String NAME = "name";

    private DeclaredNormalizerSteps() {}

    @NormalizerStep("UPPERCASE_NAMES")
    public static class UppercaseNames implements Step<JsonNode, JsonNode> {
        @Override
        public JsonNode apply(JsonNode document) {
            eachNamed(document, item -> item.put(NAME, item.get(NAME).asText().toUpperCase()));
            return document;
        }
    }

    @NormalizerStep("ADD_GREETING")
    public static class AddGreeting implements Step<JsonNode, JsonNode> {
        @Override
        public JsonNode apply(JsonNode document) {
            eachNamed(document, item -> item.put("greeting", "hello " + item.get(NAME).asText()));
            return document;
        }
    }

    @NormalizerStep("NAMES_ONLY")
    public static class NamesOnly implements Step<Stream<JsonNode>, Stream<String>> {
        @Override
        public Stream<String> apply(Stream<JsonNode> records) {
            return records.map(item -> item.get(NAME).asText());
        }
    }

    @NormalizerStep("TEXT_TO_BYTES")
    public static class TextToBytes implements Step<Stream<String>, Stream<byte[]>> {
        @Override
        public Stream<byte[]> apply(Stream<String> texts) {
            return texts.map(text -> text.getBytes(StandardCharsets.UTF_8));
        }
    }

    @NormalizerStep("LINES")
    public static class Lines implements Step<byte[], Stream<String>> {
        @Override
        public Stream<String> apply(byte[] payload) {
            return new String(payload, StandardCharsets.UTF_8).lines();
        }
    }

    @NormalizerStep("READ_YAML")
    public static class ReadYaml implements Step<byte[], JsonNode> {
        @Override
        public JsonNode apply(byte[] payload) {
            return MAPPER.valueToTree(new Yaml().load(new String(payload, StandardCharsets.UTF_8)));
        }
    }

    private static void eachNamed(JsonNode node, Consumer<ObjectNode> action) {
        if (node instanceof ObjectNode object && object.hasNonNull(NAME)) action.accept(object);
        node.forEach(child -> eachNamed(child, action));
    }
}
