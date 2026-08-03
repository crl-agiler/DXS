package eu.unicredit.document.dxstraceinfo.tools;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Optional;

public class JsonUtils {

  /**
   * An initialized Jackson ObjectMapper for JSON with: <br>
   * - DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS = true, to avoid rounding errors <br>
   * - DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES = false, to allow backward-compatibility.
   */
  public static final ObjectMapper mapper = JsonMapper.builder()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .build()
      .registerModule(new JavaTimeModule());

  private JsonUtils() {
  }

  /**
   * Returns json representation.
   */
  public static <T> String toJson(T object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception e) {
      throw new RuntimeException("Cannot print as json string the object :[" + object + "]", e);
    }
  }

  /**
   * Returns json representation as utf8 byte array.
   * More efficient than toJson(object).getBytes(StandardCharsets.UTF_8).
   */
  public static <T> byte[] toJsonUtf8Bytes(T object) {
    try {
      return mapper.writeValueAsBytes(object);
    } catch (Exception e) {
      throw new RuntimeException("Cannot print as json UTF-8 byte array the object :[" + object + "]", e);
    }
  }

  /**
   * Returns a POJO built from json.
   */
  public static <T> T fromJson(String json, Class<T> clazz) throws ParsingException {
    try {
      return mapper.readValue(json, clazz);
    } catch (Exception e) {
      throw new ParsingException("Cannot construct from json: \n```\n" + json + "\n```\n", e);
    }
  }

  /**
   * Returns a POJO built from json.
   */
  public static <T> T fromJson(String json, TypeReference<T> type) throws ParsingException {
    try {
      return mapper.readValue(json, type);
    } catch (Exception e) {
      throw new ParsingException("Cannot construct from json: \n```\n" + json + "\n```\n", e);
    }
  }

  /**
   * From YAML to JSON.
   */
  public static String yamlToJson(String yaml) throws ParsingException {
    try {
      JsonNode jsonSchema = YamlUtils.fromYaml(yaml, JsonNode.class);
      return mapper.writeValueAsString(jsonSchema);
    } catch (Exception e) {
      throw new ParsingException("Cannot construct from yaml: \n```\n" + yaml + "\n```\n", e);
    }
  }

  /**
   * Get Jackson Json Factory, to create a JsonParser.
   */
  public static JsonFactory getJsonFactory() {
    return new JsonFactory(mapper);
  }

  /**
   * Create an empty Jackson ObjectNode.
   */
  public static ObjectNode createEmptyObjectNode() {
    return mapper.createObjectNode();
  }

  /**
   * Parse a leaf field in a flattened json node.
   *
   * @param jn    input json node
   * @param field field name
   * @return string representation of the field
   * @throws ParsingException if not found
   */
  public static String parseField(JsonNode jn, String field) throws ParsingException {
    return Optional.ofNullable(jn.get(field)).orElseThrow(() ->
        new ParsingException(field + " not found for \n```\n" + jn.toPrettyString() + "\n```\n")).asText();
  }

  /**
   * Recursively merges two JsonNodes. Values in "update" overwrite those in "base" for matching fields.
   */
  public static JsonNode mergeJsonNodesWithArrayMergeByName(JsonNode baseNode, JsonNode updateNode) {
    if (baseNode.isObject() && updateNode.isObject()) {
      ObjectNode baseObjectNode = (ObjectNode) baseNode;
      ObjectNode updateObjectNode = (ObjectNode) updateNode;

      updateObjectNode.fieldNames().forEachRemaining(fieldName -> {
        JsonNode updateFieldValue = updateObjectNode.get(fieldName);
        JsonNode baseFieldValue = baseObjectNode.get(fieldName);

        // Recursively merge if both fields are objects, else overwrite or retain base if update is null
        if (baseFieldValue != null && updateFieldValue != null &&
            updateFieldValue.isObject() && baseFieldValue.isObject()) {
          baseObjectNode.set(fieldName, mergeJsonNodesWithArrayMergeByName(baseFieldValue, updateFieldValue));
        } else if (baseFieldValue != null && updateFieldValue != null &&
            updateFieldValue.isArray() && baseFieldValue.isArray()) {
          baseObjectNode.set(fieldName,
              mergeArrayNodesBySelectorField((ArrayNode) baseFieldValue, (ArrayNode) updateFieldValue, "name"));
        } else if (updateFieldValue != null && !updateFieldValue.isNull()) {
          baseObjectNode.set(fieldName, updateFieldValue);
        } // Retain base value if update is null
      });

      return baseObjectNode;
    } else {
      return updateNode.isNull() ? baseNode : updateNode;
    }
  }

  /**
   * Merges two ArrayNodes. Elements in "updateArray" are searched in "baseArray"
   * by the element field {{selectorField}}>
   * If found, merge the elements, otherwise put the "updateArray" element directly.
   * Base array elements that are not present in update array are not selected.
   */
  public static JsonNode mergeArrayNodesBySelectorField(
      ArrayNode baseArray, ArrayNode updateArray, String selectorField) {
    ObjectNode baseElementsMap = YamlUtils.mapper.createObjectNode();
    ArrayNode mergedArray = YamlUtils.mapper.createArrayNode();

    // Create a map of elements in the base array by the selector field
    for (JsonNode baseElement : baseArray) {
      if (baseElement.has(selectorField)) {
        baseElementsMap.set(baseElement.get(selectorField).asText(), baseElement);
      }
    }

    // Iterate over update array elements and merge or add them
    for (JsonNode updateElement : updateArray) {
      if (updateElement.has(selectorField)) {
        String key = updateElement.get(selectorField).asText();
        JsonNode baseElement = baseElementsMap.get(key);

        if (baseElement != null && baseElement.isObject() && updateElement.isObject()) {
          mergedArray.add(mergeJsonNodesWithArrayMergeByName(baseElement, updateElement));
        } else {
          mergedArray.add(updateElement);
        }
      } else {
        mergedArray.add(updateElement);
      }
    }

    return mergedArray;
  }
}
