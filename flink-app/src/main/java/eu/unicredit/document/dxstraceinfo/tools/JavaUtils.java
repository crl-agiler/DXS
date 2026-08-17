package eu.unicredit.document.dxstraceinfo.tools;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.ExceptionUtils;
import org.jooq.lambda.tuple.Tuple2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class JavaUtils {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private static final JavaPropsMapper propsMapper = (JavaPropsMapper) JavaPropsMapper.builder()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .build()
      .registerModule(new JavaTimeModule());

  private JavaUtils() {
  }

  /**
   * Returns a string representation of the given exception (stack trace), limited to the specified maximum characters.
   *
   * @param e       the exception to stringify
   * @param maxChar the maximum number of characters in the string representation. Includes the ellipsis!
   * @return the string representation of the exception, limited to the specified maximum characters
   */
  public static String stringifyException(Throwable e, int maxChar) {
    final String string = ExceptionUtils.stringifyException(e);
    return StringUtils.abbreviate(string, maxChar);
  }

  /**
   * Reads a string from a local file located at {@code path} in resources.
   *
   * @throws RuntimeException if the file does not exist or can't be loaded
   */
  public static String readStringUtf8FromResources(String path) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(JavaUtils.class.getClassLoader().getResource(path)),
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Build a properties object from a Jackson POJO.
   */
  public static <T> Properties propertiesFromJacksonPojo(T pojo) {
    try {
      return propsMapper.writeValueAsProperties(pojo);
    } catch (Exception e) {
      throw new RuntimeException("Cannot convert pojo to java Properties :[" + pojo + "]", e);
    }
  }

  /**
   * Build a properties object from a {@code Map<String, String>}.
   */
  public static Properties propertiesFromMap(Map<String, String> map) {
    Properties properties = new Properties();
    properties.putAll(map);
    return properties;
  }

  /**
   * Build a {@code LinkedHashMap<String, String>} from a Properties object.
   */
  public static LinkedHashMap<String, String> mapFromProperties(Properties properties) {
    final LinkedHashMap<String, String> map = new LinkedHashMap<>();
    for (final String name : properties.stringPropertyNames()) {
      map.put(name, properties.getProperty(name));
    }
    return map;
  }

  /**
   * LinkedHashMap collector.
   */
  public static <K, V> Collector<Tuple2<K, V>, ?, LinkedHashMap<K, V>> linkedHashMapCollector() {
    return Collectors.toMap(
        tuple -> tuple.v1,        // Key mapper
        tuple -> tuple.v2,        // Value mapper
        (x, y) -> y,              // Merge function (not needed, but handles duplicates)
        LinkedHashMap::new        // Map supplier
    );
  }

  /**
   * LinkedHashMap collector.
   */
  public static <K, V> Collector<Tuple2<K, V>, ?, HashMap<K, V>> hashMapCollector() {
    return Collectors.toMap(
        tuple -> tuple.v1,        // Key mapper
        tuple -> tuple.v2,        // Value mapper
        (x, y) -> y,              // Merge function (not needed, but handles duplicates)
        HashMap::new              // Map supplier
    );
  }
}
