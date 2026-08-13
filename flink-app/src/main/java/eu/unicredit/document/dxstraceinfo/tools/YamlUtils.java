package eu.unicredit.document.dxstraceinfo.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.util.Objects;

/**
 * Utility methods for YAML serialization, deserialization and merging.
 */
public final class YamlUtils {

  /**
   * Configured YAML object mapper.
   *
   * <p>Configuration:</p>
   * <ul>
   *     <li>Uses {@link java.math.BigDecimal} for floating-point values.</li>
   *     <li>Ignores unknown properties for backward compatibility.</li>
   *     <li>Supports Java date and time types.</li>
   *     <li>Does not write YAML document start markers.</li>
   *     <li>Does not minimize YAML quotes.</li>
   *     <li>Does not serialize dates as numeric timestamps.</li>
   * </ul>
   */
  private static final ObjectMapper MAPPER = createMapper();

  private YamlUtils() {
    throw new IllegalStateException(
            "Utility class cannot be instantiated"
    );
  }

  private static ObjectMapper createMapper() {
    YAMLFactory yamlFactory = YAMLFactory.builder()
            .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
            .disable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .build();

    return JsonMapper.builder(yamlFactory)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .addModule(new JavaTimeModule())
            .build();
  }

  /**
   * Serializes an object to YAML.
   *
   * @param object object to serialize
   * @param <T> object's type
   * @return YAML representation
   * @throws ParsingException if serialization fails
   */
  public static <T> String toYaml(T object) {
    Objects.requireNonNull(object, "object cannot be null");

    try {
      return MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException exception) {
      throw new ParsingException(
              "Cannot serialize object of type "
                      + object.getClass().getName()
                      + " to YAML",
              exception
      );
    }
  }

  /**
   * Deserializes a YAML document into the requested type.
   *
   * @param yaml YAML document
   * @param type requested result type
   * @param <T> result type
   * @return deserialized object
   * @throws ParsingException if deserialization fails
   */
  public static <T> T fromYaml(
          String yaml,
          Class<T> type) {

    Objects.requireNonNull(yaml, "yaml cannot be null");
    Objects.requireNonNull(type, "type cannot be null");

    if (yaml.trim().isEmpty()) {
      throw new ParsingException(
              "Cannot deserialize an empty YAML document as "
                      + type.getName()
      );
    }

    try {
      return MAPPER.readValue(yaml, type);
    } catch (JsonProcessingException exception) {
      throw new ParsingException(
              "Cannot deserialize YAML document as "
                      + type.getName()
                      + formatContent(yaml),
              exception
      );
    }
  }

  /**
   * Deserializes a YAML document into a generic type.
   *
   * <p>This overload supports parameterized types such as:</p>
   *
   * <pre>{@code
   * List<ModuleConfig> modules = YamlUtils.fromYaml(
   *     yaml,
   *     new TypeReference<List<ModuleConfig>>() {}
   * );
   * }</pre>
   *
   * @param yaml YAML document
   * @param typeReference requested generic type
   * @param <T> result type
   * @return deserialized object
   * @throws ParsingException if deserialization fails
   */
  public static <T> T fromYaml(
          String yaml,
          TypeReference<T> typeReference) {

    Objects.requireNonNull(yaml, "yaml cannot be null");
    Objects.requireNonNull(
            typeReference,
            "typeReference cannot be null"
    );

    if (yaml.trim().isEmpty()) {
      throw new ParsingException(
              "Cannot deserialize an empty YAML document as "
                      + typeReference.getType()
      );
    }

    try {
      return MAPPER.readValue(yaml, typeReference);
    } catch (JsonProcessingException exception) {
      throw new ParsingException(
              "Cannot deserialize YAML document as "
                      + typeReference.getType()
                      + formatContent(yaml),
              exception
      );
    }
  }

  /**
   * Converts a JSON document to YAML.
   *
   * @param json JSON document
   * @return YAML representation
   * @throws ParsingException if conversion fails
   */
  public static String jsonToYaml(String json) {
    Objects.requireNonNull(json, "json cannot be null");

    if (json.trim().isEmpty()) {
      throw new ParsingException(
              "Cannot convert an empty JSON document to YAML"
      );
    }

    final JsonNode jsonNode;

    try {
      jsonNode = JsonUtils.fromJson(json, JsonNode.class);
    } catch (ParsingException exception) {
      throw new ParsingException(
              "Cannot parse JSON document"
                      + formatContent(json),
              exception
      );
    }

    try {
      return MAPPER.writeValueAsString(jsonNode);
    } catch (JsonProcessingException exception) {
      throw new ParsingException(
              "Cannot serialize parsed JSON document to YAML",
              exception
      );
    }
  }

  /**
   * Merges two YAML documents and converts the result to the requested type.
   *
   * <p>Merge rules:</p>
   * <ul>
   *     <li>
   *         Arrays whose elements contain a {@code name} field are merged
   *         by name.
   *     </li>
   *     <li>
   *         A child array element overrides the corresponding parent
   *         element.
   *     </li>
   *     <li>
   *         Child elements without a corresponding parent element are added.
   *     </li>
   *     <li>
   *         When an array is present in both documents, parent elements that
   *         are not present in the child array are excluded.
   *     </li>
   *     <li>
   *         When a parent array is absent from the child document, the parent
   *         array is preserved.
   *     </li>
   * </ul>
   *
   * @param parent parent YAML document
   * @param child child YAML document
   * @param returnType requested result type
   * @param <T> result type
   * @return merged and deserialized object
   * @throws ParsingException if parsing, merging or conversion fails
   */
  public static <T> T merge(
          String parent,
          String child,
          Class<T> returnType) {

    Objects.requireNonNull(parent, "parent cannot be null");
    Objects.requireNonNull(child, "child cannot be null");
    Objects.requireNonNull(
            returnType,
            "returnType cannot be null"
    );

    JsonNode mergedNode = mergeNodes(parent, child);

    try {
      return MAPPER.treeToValue(mergedNode, returnType);
    } catch (JsonProcessingException exception) {
      throw new ParsingException(
              "Cannot convert merged YAML document to "
                      + returnType.getName()
                      + formatNode(mergedNode),
              exception
      );
    }
  }

  /**
   * Merges two YAML documents and converts the result to a generic type.
   *
   * <p>This overload supports parameterized return types such as
   * {@code List<ModuleConfig>} or {@code Map<String, ModuleConfig>}.</p>
   *
   * @param parent parent YAML document
   * @param child child YAML document
   * @param returnType requested generic result type
   * @param <T> result type
   * @return merged and deserialized object
   * @throws ParsingException if parsing, merging or conversion fails
   */
  public static <T> T merge(
          String parent,
          String child,
          TypeReference<T> returnType) {

    Objects.requireNonNull(parent, "parent cannot be null");
    Objects.requireNonNull(child, "child cannot be null");
    Objects.requireNonNull(
            returnType,
            "returnType cannot be null"
    );

    JsonNode mergedNode = mergeNodes(parent, child);

    try {
      return MAPPER.readerFor(returnType)
              .readValue(mergedNode);
    } catch (IOException exception) {
      throw new ParsingException(
              "Cannot convert merged YAML document to "
                      + returnType.getType()
                      + formatNode(mergedNode),
              exception
      );
    }
  }

  /**
   * Parses and merges two YAML documents, returning their JSON tree.
   *
   * @param parent parent YAML document
   * @param child child YAML document
   * @return merged JSON tree
   */
  private static JsonNode mergeNodes(
          String parent,
          String child) {

    JsonNode parentNode = parseYamlNode(parent, "parent");
    JsonNode childNode = parseYamlNode(child, "child");

    final JsonNode mergedNode;

    try {
      mergedNode =
              JsonUtils.mergeJsonNodesWithArrayMergeByName(
                      parentNode,
                      childNode
              );
    } catch (RuntimeException exception) {
      throw new ParsingException(
              "Cannot merge parent and child YAML documents",
              exception
      );
    }

    if (mergedNode == null || mergedNode.isNull()) {
      throw new ParsingException(
              "YAML merge produced a null result"
      );
    }

    return mergedNode;
  }

  /**
   * Parses a YAML document as a JSON tree.
   *
   * <p>An empty YAML document is interpreted as an empty object.</p>
   *
   * @param yaml YAML document
   * @param sourceName source description used in error messages
   * @return parsed JSON tree
   */
  private static JsonNode parseYamlNode(
          String yaml,
          String sourceName) {

    if (yaml.trim().isEmpty()) {
      return MAPPER.createObjectNode();
    }

    final JsonNode node;

    try {
      node = MAPPER.readTree(yaml);
    } catch (JsonProcessingException exception) {
      throw new ParsingException(
              "Cannot parse " + sourceName
                      + " YAML document"
                      + formatContent(yaml),
              exception
      );
    }

    if (node == null || node.isNull()) {
      return MAPPER.createObjectNode();
    }

    if (!node.isContainerNode()) {
      throw new ParsingException(
              "The " + sourceName
                      + " YAML root must be an object or an array, "
                      + "but was "
                      + node.getNodeType()
      );
    }

    return node;
  }

  /**
   * Formats textual content for an exception without producing excessively
   * large error messages.
   */
  private static String formatContent(String content) {
    final int maximumLength = 4_000;

    String printableContent = content;

    if (content.length() > maximumLength) {
      printableContent = content.substring(0, maximumLength)
              + System.lineSeparator()
              + "... content truncated ...";
    }

    return ":"
            + System.lineSeparator()
            + "```"
            + System.lineSeparator()
            + printableContent
            + System.lineSeparator()
            + "```";
  }

  private static String formatNode(JsonNode node) {
    try {
      return formatContent(
              MAPPER.writerWithDefaultPrettyPrinter()
                      .writeValueAsString(node)
      );
    } catch (JsonProcessingException exception) {
      return "";
    }
  }
}