package eu.unicredit.document.dxstraceinfo.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class YamlUtils {

  /**
   * An initialized Jackson ObjectMapper for YAML with: <br>
   * - DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS = true, to avoid rounding errors <br>
   * - DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES = false, to allow backward-compatibility.
   */
  public static final ObjectMapper mapper = JsonMapper.builder(new YAMLFactory()
          .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
          .disable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
          .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .build()
      .registerModule(new JavaTimeModule());

  private YamlUtils() {
  }

  /**
   * Returns yaml representation.
   */
  public static <T> String toYaml(T object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception e) {
      throw new RuntimeException("Cannot print as yaml string the object :[" + object + "]", e);
    }
  }

  /**
   * Returns a POJO built from yaml.
   */
  public static <T> T fromYaml(String yaml, Class<T> clazz) throws ParsingException {
    try {
      return mapper.readValue(yaml, clazz);
    } catch (Exception e) {
      throw new ParsingException("Cannot construct from yaml: \n```\n" + yaml + "\n```\n", e);
    }
  }

  /**
   * From JSON to YAML.
   */
  public static String jsonToYaml(String json) throws ParsingException {
    try {
      JsonNode jsonSchema = JsonUtils.fromJson(json, JsonNode.class);
      return mapper.writeValueAsString(jsonSchema);
    } catch (JsonProcessingException e) {
      throw new ParsingException("Cannot construct from json: \n```\n" + json + "\n```\n", e);
    }
  }

}
