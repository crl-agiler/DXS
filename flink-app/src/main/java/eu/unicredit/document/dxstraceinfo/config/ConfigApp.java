package eu.unicredit.document.dxstraceinfo.config;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import eu.unicredit.document.dxstraceinfo.tools.YamlUtils;
import java.io.Serializable;
import java.util.List;
import javax.validation.Valid;
import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Configurations for {@code eu.unicredit.customers.yfareplatforming.App}.
 */
// lombok annotations
@Value
@Jacksonized
@Builder(toBuilder = true)
// jackson annotations
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonClassDescription("Configurations for `Flink job`")
@JsonTypeName(ConfigApp.TYPE)
public class ConfigApp implements Serializable, JakartaValidable {

  /**
   * This fields embeds the type name of the serialized entity. Add a version semantic if needed.
   * Note: use @Valid for fields with custom types. @AssertTrue/False methods must start with "is".
   */
  @JsonIgnore
  public static final String TYPE = "dxs-traceinfo-flink-streaming-wl_config";

  @JsonIgnore
  String bucketName;

  @NotNull
  @Valid
  @JsonProperty(value = "projectId", required = true)
  @JsonPropertyDescription("GCP Project ID")
  String projectId;

  @NotNull
  @Valid
  @JsonProperty(value = "flink", required = true)
  @JsonPropertyDescription("Flink Configurations")
  ConfigFlink flinkConfig;

  @NotNull
  @Valid
  @JsonProperty(value = "schemaRegistry", required = true)
  @JsonPropertyDescription("Confluent Schema Registry Configurations")
  ConfigSchemaRegistry schemaRegistryConfig;

  @NotNull
  @Valid
  @JsonProperty(value = "kafka", required = true)
  @JsonPropertyDescription("Kafka Configurations")
  ConfigKafka kafkaConfig;

  @NotNull
  @Valid
  @JsonProperty(value = "catalog", required = true)
  @JsonPropertyDescription("Catalog Configurations")
  ConfigIcebergCatalog configIcebergCatalog;

  @Valid
  @JsonProperty(value = "placeholders", required = true)
  @JsonPropertyDescription("Placeholders to substitute in yaml file")
  List<ConfigPlaceholder> placeholderConfigs;


  @Builder.Default
  @Valid
  @JsonProperty(value = "debugMode", defaultValue = "false")
  @JsonPropertyDescription("Enable debug mode to help debugging a deployed App")
  boolean debugMode = false;


  @JsonIgnore
  @Override
  public void validate() throws ValidationException {
    ValidatorProxy.validate(this);
  }

  @Override
  public String toString() {
    return YamlUtils.toYaml(this);
  }
}
